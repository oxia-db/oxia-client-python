// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The client thread behind a Python `Client`.
//!
//! The native client is single-threaded: it lives on a thread of its own, in
//! a compio runtime, and every operation runs there. A Python method hands a
//! job to that thread and blocks until it answers, with the GIL released
//! (see [`crate::Client`]), so other Python threads keep running and their
//! operations batch together with this one.

use std::sync::Mutex;
use std::sync::mpsc as sync_mpsc;
use std::thread::JoinHandle;

use futures::channel::mpsc;
use futures::future::LocalBoxFuture;
use futures::{SinkExt, StreamExt};
use oxia::{OxiaClient, OxiaClientBuilder, OxiaError};

use crate::streams::Subscription;

/// An operation to run on the client's thread.
type Job = Box<dyn FnOnce(OxiaClient) -> LocalBoxFuture<'static, ()> + Send>;

/// How many items a subscription buffers ahead of its Python consumer.
const STREAM_BUFFER: usize = 100;

pub(crate) struct Worker {
    jobs: mpsc::UnboundedSender<Job>,
    /// Taken by [`Worker::close`], so that closing twice joins once.
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Worker {
    /// Builds the client on a thread of its own and returns once it is
    /// connected, or with the error that stopped it.
    pub(crate) fn start(builder: OxiaClientBuilder) -> Result<Worker, OxiaError> {
        let (ready_tx, ready_rx) = sync_mpsc::channel::<Result<(), OxiaError>>();
        let (jobs, mut job_rx) = mpsc::unbounded::<Job>();

        let thread = std::thread::Builder::new()
            .name("oxia-client".to_string())
            .spawn(move || {
                let runtime = match compio::runtime::Runtime::new() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready_tx.send(Err(OxiaError::Disconnected(format!(
                            "cannot start the compio runtime: {err}"
                        ))));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let client = match builder.build().await {
                        Ok(client) => client,
                        Err(err) => {
                            let _ = ready_tx.send(Err(err));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));
                    while let Some(job) = job_rx.next().await {
                        compio::runtime::spawn(job(client.clone())).detach();
                    }
                });
            })
            .map_err(|err| OxiaError::Disconnected(format!("cannot start a thread: {err}")))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Worker {
                jobs,
                thread: Mutex::new(Some(thread)),
            }),
            // The thread has already returned, so joining it is immediate.
            Ok(Err(err)) => {
                let _ = thread.join();
                Err(err)
            }
            Err(_) => {
                let _ = thread.join();
                Err(OxiaError::Closed)
            }
        }
    }

    /// Runs `op` on the client's thread and waits for its result.
    ///
    /// The caller releases the GIL around this.
    pub(crate) fn run<T, F, Fut>(&self, op: F) -> Result<T, OxiaError>
    where
        T: Send + 'static,
        F: FnOnce(OxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, OxiaError>> + 'static,
    {
        let (tx, rx) = sync_mpsc::channel();
        let job: Job = Box::new(move |client| {
            Box::pin(async move {
                let _ = tx.send(op(client).await);
            })
        });
        if self.jobs.unbounded_send(job).is_err() {
            return Err(OxiaError::Closed);
        }
        // The job is dropped unanswered when the client's thread has stopped.
        rx.recv().unwrap_or(Err(OxiaError::Closed))
    }

    /// Opens a subscription or a streaming read and returns a handle the
    /// Python thread can pull from.
    ///
    /// `open` runs on the client's thread and the stream it returns stays
    /// there — none of the native stream types is `Send`. This blocks until
    /// the stream is open, or until opening it fails, and then leaves a task
    /// behind that forwards items into the handle's channel, which
    /// backpressures once [`STREAM_BUFFER`] of them are waiting.
    pub(crate) fn open_stream<T, F, Fut, S>(&self, open: F) -> Result<Subscription<T>, OxiaError>
    where
        T: Send + 'static,
        F: FnOnce(OxiaClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, OxiaError>> + 'static,
        S: futures::Stream<Item = Result<T, OxiaError>> + 'static,
    {
        let (ready_tx, ready_rx) = sync_mpsc::channel::<Result<(), OxiaError>>();
        let (mut items_tx, items_rx) = mpsc::channel::<Result<T, OxiaError>>(STREAM_BUFFER);
        let (cancel_tx, cancel_rx) = futures::channel::oneshot::channel::<()>();

        let job: Job = Box::new(move |client| {
            Box::pin(async move {
                let stream = match open(client).await {
                    Ok(stream) => {
                        let _ = ready_tx.send(Ok(()));
                        stream
                    }
                    Err(err) => {
                        let _ = ready_tx.send(Err(err));
                        return;
                    }
                };
                // Fused so that `select!` can poll it: `Next` is only a
                // `FusedFuture` over a `FusedStream`.
                let stream = stream.fuse();
                futures::pin_mut!(stream);
                let mut cancelled = cancel_rx;
                loop {
                    futures::select! {
                        // The handle was closed or dropped. Returning drops
                        // the native stream, which releases the subscription.
                        _ = cancelled => return,
                        item = stream.next() => match item {
                            // The consumer stopped reading: same as cancelled.
                            Some(item) => if items_tx.send(item).await.is_err() {
                                return;
                            },
                            // The client was closed, or the scan is over.
                            None => return,
                        },
                    }
                }
            })
        });

        if self.jobs.unbounded_send(job).is_err() {
            return Err(OxiaError::Closed);
        }
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Subscription::new(items_rx, cancel_tx)),
            Ok(Err(err)) => Err(err),
            // The client's thread stopped before it could answer.
            Err(_) => Err(OxiaError::Closed),
        }
    }

    /// Flushes and shuts the native client down, then stops its thread.
    ///
    /// Idempotent: closing an already closed client succeeds.
    pub(crate) fn close(&self) -> Result<(), OxiaError> {
        let closed = self.run(|client| async move { client.close().await });
        // Stop the thread even if the close itself failed, so that no client
        // can leave one behind.
        self.jobs.close_channel();
        if let Some(thread) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = thread.join();
        }
        match closed {
            // The thread was already gone: this is a second close.
            Err(OxiaError::Closed) => Ok(()),
            other => other,
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
