use crate::{
    BoxError,
    body::{self, Body},
    status::Status,
};
use bytes::{Buf, Bytes};
use http::{HeaderMap, Request, Response};
use http_body::{Frame, SizeHint};
use http_body_util::combinators::UnsyncBoxBody;
use std::{
    error::Error as StdError,
    future, mem,
    pin::Pin,
    sync::Arc,
    task::{self, Poll, ready},
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tower_service::Service;

type BoxFuture = Pin<Box<dyn future::Future<Output = Result<Response<Body>, BoxError>> + Send>>;

/// Returns true if the error indicates a request that was never processed by
/// the server and is unconditionally safe to retry per RFC 9113 §6.8 and gRFC A6.
///
/// This covers four cases:
/// - GOAWAY with NO_ERROR: the server is shutting down gracefully, streams that
///   tried to open after conn_error was set get this error
/// - REFUSED_STREAM: streams that had HEADERS on the wire but whose IDs were
///   above last_stream_id get RST_STREAM with REFUSED_STREAM
/// - Canceled: requests queued in hyper's dispatch channel that never reached
///   h2::SendRequest::send_request() — the channel was dropped when the
///   connection task exited on Dispatched::Shutdown
/// - DispatchGone: the h2 connection task exited (e.g. after draining a GOAWAY)
///   while requests were still queued in hyper's dispatch channel — the
///   Callback::drop() impl sends this error to waiting callers
fn is_retryable_stream_rejection(err: &BoxError) -> bool {
    // Walk the full source chain — the hyper::Error may be wrapped in
    // tonic::transport::Error or other layers.
    let mut current: Option<&(dyn StdError + 'static)> = Some(err.as_ref());
    while let Some(e) = current {
        if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
            // GOAWAY with NO_ERROR
            if hyper_err.h2_go_away_reason() == Some(0) {
                return true;
            }

            // Canceled: request never left hyper's dispatch channel
            if hyper_err.is_canceled() {
                return true;
            }

            // DispatchGone: connection task exited while requests were queued.
            if hyper_err.is_dispatch_gone() {
                return true;
            }
        }

        if let Some(h2_err) = e.downcast_ref::<h2::Error>()
            && h2_err.is_remote()
            && h2_err.reason() == Some(h2::Reason::REFUSED_STREAM)
        {
            return true;
        }

        current = e.source();
    }

    false
}

/// Service layer that transparently retries requests rejected before processing.
///
/// When an HTTP/2 server sends GOAWAY with NO_ERROR or resets a stream with
/// REFUSED_STREAM, the request was never processed by the server. Per gRFC A6,
/// these are unconditionally safe to retry. This layer detects such failures
/// and retries on a fresh connection.
///
/// Request bodies are made cloneable via [`SharedBody`] so they can be replayed.
/// The clone is dropped as soon as the first attempt succeeds (Response-Headers
/// received = RPC committed per gRFC A6), freeing the buffer.
pub(crate) struct GoawayRetry<S> {
    inner: S,
}

impl<S: Clone> Clone for GoawayRetry<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> GoawayRetry<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<Request<Body>> for GoawayRetry<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = BoxFuture;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Make the body cloneable for potential retry.
        let (parts, body) = req.into_parts();
        let mut svc = {
            let src = self.inner.clone();
            mem::replace(&mut self.inner, src)
        };
        let body_shared = Arc::new(Mutex::new(Some(BodyShared::new(body))));

        Box::pin(async move {
            let mut attempts = 0;
            let mut shared = body_shared.clone().lock_owned().await;

            loop {
                let req = Request::from_parts(
                    parts.clone(),
                    Body::new(BodyRef::Uncommitted(Uncommitted {
                        shared,
                        read_offset: Some(ReadOffset { index: 0, bytes: 0 }),
                    })),
                );

                tracing::trace!(attempt = attempts, "goaway_retry: calling inner service");
                let ret = svc.call(req).await;

                if let Err(ref e) = ret {
                    let retryable = is_retryable_stream_rejection(e);
                    tracing::debug!(
                        attempt = attempts,
                        retryable,
                        error = %e,
                        error_debug = ?e,
                        "goaway_retry: call failed"
                    );
                }

                // If the request was rejected before processing (GOAWAY or
                // REFUSED_STREAM), retry on a fresh connection. We may need
                // multiple attempts because the underlying Reconnect layer
                // might still dispatch to the dying connection on the first
                // retry (the h2 connection stays alive to drain accepted
                // streams, so hyper's is_closed() returns false briefly).
                if ret.as_ref().is_err_and(is_retryable_stream_rejection) && attempts < 16 {
                    shared = body_shared.clone().lock_owned().await;
                    if shared.is_some() {
                        attempts += 1;
                        tracing::debug!(
                            attempt = attempts,
                            "goaway_retry: retrying, waiting for poll_ready"
                        );
                        match future::poll_fn(|cx| svc.poll_ready(cx)).await {
                            Ok(()) => {
                                tracing::debug!(
                                    attempt = attempts,
                                    "goaway_retry: poll_ready succeeded, retrying"
                                );
                            }
                            Err(e) => {
                                tracing::debug!(
                                    attempt = attempts,
                                    error = %e,
                                    error_debug = ?e,
                                    "goaway_retry: poll_ready failed on retry"
                                );
                                return Err(e);
                            }
                        }
                        continue;
                    } else {
                        tracing::debug!(
                            attempt = attempts,
                            "goaway_retry: body already committed, cannot retry"
                        );
                    }
                }

                return ret;
            }
        })
    }
}

struct BodyShared {
    frames: Vec<Bytes>,
    byte_count: usize,
    trailers: Option<HeaderMap>,
    body: Option<UnsyncBoxBody<Bytes, Status>>,
}

impl BodyShared {
    fn new(body: Body) -> Self {
        let body = match body.kind {
            body::Kind::Empty => None,
            body::Kind::Wrap(body) => Some(body),
        };

        Self {
            frames: vec![],
            byte_count: 0,
            trailers: None,
            body,
        }
    }
}

enum BodyRef {
    Uncommitted(Uncommitted),
    Owned(Option<UnsyncBoxBody<Bytes, Status>>),
}

struct Uncommitted {
    shared: OwnedMutexGuard<Option<BodyShared>>,
    read_offset: Option<ReadOffset>,
}

#[derive(Default)]
struct ReadOffset {
    index: usize,
    bytes: usize,
}

impl Unpin for BodyRef {}

impl http_body::Body for BodyRef {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let state = self.get_mut();

        let uncommitted = loop {
            let uncommitted = match state {
                BodyRef::Owned(Some(body)) => {
                    return Pin::new(body).poll_frame(cx);
                }
                BodyRef::Owned(None) => {
                    return Poll::Ready(None);
                }
                BodyRef::Uncommitted(uncommitted) => uncommitted,
            };

            let shared = uncommitted.shared.as_mut().unwrap();

            if uncommitted
                .read_offset
                .take_if(|o| o.index == shared.frames.len())
                .is_some()
                && let Some(trailers) = &shared.trailers
            {
                return Poll::Ready(Some(Ok(Frame::trailers(trailers.clone()))));
            }

            if let Some(offset) = &mut uncommitted.read_offset {
                let buf = shared.frames[offset.index].clone();
                offset.index += 1;
                offset.bytes += buf.len();
                return Poll::Ready(Some(Ok(Frame::data(buf))));
            }

            if Arc::strong_count(OwnedMutexGuard::mutex(&uncommitted.shared)) == 1 {
                // commit: we hold the last reference
                *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
                continue;
            }

            break uncommitted;
        };

        let shared = uncommitted.shared.as_mut().unwrap();

        let frame = match &mut shared.body {
            None => None,
            Some(body) => ready!(Pin::new(body).poll_frame(cx)),
        };

        let frame = match frame {
            Some(Ok(frame)) => {
                if shared.body.as_ref().is_some_and(|b| b.is_end_stream()) {
                    // body exhausted, drop it
                    shared.body = None;
                }

                frame
            }
            Some(Err(err)) => {
                // commit: body errored
                *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
                return Poll::Ready(Some(Err(err)));
            }
            None => {
                // body exhausted, drop it
                shared.body = None;
                return Poll::Ready(None);
            }
        };

        if let Some(data) = frame.data_ref() {
            if let Some(new_len) = data.remaining().checked_add(shared.byte_count)
                && new_len < (1 << 18)
            {
                shared.byte_count = new_len;
                shared.frames.push(data.clone());
            } else {
                // commit: exceeded max buffer size
                *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
            }
        } else if let Some(trailers) = frame.trailers_ref() {
            assert!(shared.trailers.is_none());
            shared.trailers = Some(trailers.clone());
        } else {
            // commit: unknown frame type
            *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
        }

        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        match self {
            BodyRef::Uncommitted(uncommitted) => {
                let shared = uncommitted.shared.as_ref().unwrap();
                uncommitted.read_offset.is_none()
                    && shared.body.as_ref().is_none_or(|b| b.is_end_stream())
            }
            BodyRef::Owned(Some(body)) => body.is_end_stream(),
            BodyRef::Owned(None) => true,
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let (bytes, body) = match self {
            BodyRef::Uncommitted(uncommitted) => {
                let shared = uncommitted.shared.as_ref().unwrap();
                let buffered = match &uncommitted.read_offset {
                    Some(offset) => shared.byte_count - offset.bytes,
                    None => 0,
                };
                (buffered as u64, &shared.body)
            }
            BodyRef::Owned(body) => (0, body),
        };

        let body = body
            .as_ref()
            .map_or(SizeHint::with_exact(0), |b| b.size_hint());

        let mut hint = SizeHint::new();

        hint.set_lower(body.lower().saturating_add(bytes));
        if let Some(body_upper) = body.upper()
            && let Some(upper) = body_upper.checked_add(bytes)
        {
            hint.set_upper(upper);
        }

        hint
    }
}
