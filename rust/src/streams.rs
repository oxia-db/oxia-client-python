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

//! The Python iterators over a native stream.
//!
//! Each of them wraps a [`Subscription`]: the receiving end of the channel a
//! task on the client's thread forwards items into, and the handle that stops
//! that task. `__next__` blocks on the channel with the GIL released, so a
//! consumer parked on a subscription does not hold up the interpreter.

use std::sync::Mutex;

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use oxia::{GetResult, Notification, OxiaError};
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;

use crate::errors::to_py_err;

/// The consumer's end of an open stream.
pub(crate) struct Subscription<T> {
    items: mpsc::Receiver<Result<T, OxiaError>>,
    /// Dropped — or sent on — to stop the task on the client's thread.
    cancel: Option<oneshot::Sender<()>>,
}

impl<T> Subscription<T> {
    pub(crate) fn new(
        items: mpsc::Receiver<Result<T, OxiaError>>,
        cancel: oneshot::Sender<()>,
    ) -> Self {
        Subscription {
            items,
            cancel: Some(cancel),
        }
    }

    /// Waits for the next item. `None` once the stream is over, which also
    /// happens when the client is closed.
    fn next(&mut self) -> Option<Result<T, OxiaError>> {
        futures::executor::block_on(self.items.next())
    }

    /// Stops the stream. Idempotent, and a stopped stream reports itself
    /// exhausted rather than failing.
    fn close(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        self.items.close();
    }
}

/// Pulls the next item, releasing the GIL while it waits.
///
/// Reports the end of the stream as `StopIteration` and an error as the
/// matching `oxia.ex` exception.
fn pull<T: Send>(py: Python<'_>, subscription: &Mutex<Subscription<T>>) -> PyResult<T> {
    let item = py.detach(|| {
        subscription
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .next()
    });
    match item {
        Some(Ok(item)) => Ok(item),
        Some(Err(err)) => Err(to_py_err(py, err)),
        None => Err(PyStopIteration::new_err(())),
    }
}

fn close<T: Send>(subscription: &Mutex<Subscription<T>>) {
    subscription
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .close();
}

/// Yields the records of a range scan, in key order across shards.
#[pyclass(module = "oxia._oxia")]
pub(crate) struct RangeScanIterator {
    subscription: Mutex<Subscription<GetResult>>,
}

#[pymethods]
impl RangeScanIterator {
    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<crate::Record> {
        Ok(crate::record_to_py(pull(py, &self.subscription)?))
    }

    fn close(&self) {
        close(&self.subscription);
    }
}

impl RangeScanIterator {
    pub(crate) fn new(subscription: Subscription<GetResult>) -> Self {
        RangeScanIterator {
            subscription: Mutex::new(subscription),
        }
    }
}

/// Yields database change notifications as
/// `(type, key, version_id, key_range_end)`.
///
/// `type` is the ordinal of `oxia.NotificationType`. A
/// `NotificationsMissed` from the server has no equivalent there, so it is
/// dropped here rather than reaching Python as a fifth kind.
#[pyclass(module = "oxia._oxia")]
pub(crate) struct NotificationIterator {
    subscription: Mutex<Subscription<Notification>>,
}

#[pymethods]
impl NotificationIterator {
    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<(u8, String, i64, Option<String>)> {
        loop {
            if let Some(item) = notification_to_py(pull(py, &self.subscription)?) {
                return Ok(item);
            }
        }
    }

    fn close(&self) {
        close(&self.subscription);
    }
}

impl NotificationIterator {
    pub(crate) fn new(subscription: Subscription<Notification>) -> Self {
        NotificationIterator {
            subscription: Mutex::new(subscription),
        }
    }
}

/// `None` for a notification Python has no type for, which the iterator skips.
fn notification_to_py(notification: Notification) -> Option<(u8, String, i64, Option<String>)> {
    match notification {
        Notification::KeyCreated { key, version_id } => {
            Some((0, key, version_id.unwrap_or(0), None))
        }
        Notification::KeyModified { key, version_id } => {
            Some((1, key, version_id.unwrap_or(0), None))
        }
        Notification::KeyDeleted { key } => Some((2, key, 0, None)),
        Notification::KeyRangeDeleted {
            key,
            key_range_last,
        } => Some((3, key, 0, key_range_last)),
        Notification::NotificationsMissed => None,
        // A kind added to the protocol after this build: skipped too, rather
        // than reaching Python as a type its enum does not have.
        _ => None,
    }
}

/// Yields the highest assigned sequence key each time the sequence advances.
#[pyclass(module = "oxia._oxia")]
pub(crate) struct SequenceUpdatesIterator {
    subscription: Mutex<Subscription<String>>,
}

#[pymethods]
impl SequenceUpdatesIterator {
    fn __iter__(this: PyRef<'_, Self>) -> PyRef<'_, Self> {
        this
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<String> {
        pull(py, &self.subscription)
    }

    fn close(&self) {
        close(&self.subscription);
    }
}

impl SequenceUpdatesIterator {
    pub(crate) fn new(subscription: Subscription<String>) -> Self {
        SequenceUpdatesIterator {
            subscription: Mutex::new(subscription),
        }
    }
}
