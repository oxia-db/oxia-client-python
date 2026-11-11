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

//! `oxia._oxia`: the native half of the Oxia Python client.
//!
//! This module is deliberately thin. It owns the client thread and the
//! conversions to and from the native types, and nothing else: the public
//! API, its documentation, its option checks and its `Version`,
//! `Notification` and exception classes all live in the `oxia` Python
//! package, which is what users import. Everything here is passed as plain
//! tuples, which that package turns into those classes.

mod errors;
mod runtime;
mod streams;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use oxia::{ComparisonType, GetResult, OxiaClientBuilder, Version};
use pyo3::prelude::*;

use crate::errors::to_py_err;
use crate::runtime::Worker;
use crate::streams::{NotificationIterator, RangeScanIterator, SequenceUpdatesIterator};

/// A `Version`, as `oxia.client._version` rebuilds it:
/// `(version_id, modifications_count, created_timestamp_ms,
/// modified_timestamp_ms, session_id, client_identity)`.
type VersionTuple = (i64, i64, u64, u64, Option<i64>, Option<String>);

/// A record: its key, its value when one was asked for and the server sent
/// it, and its version.
type Record = (String, Option<Vec<u8>>, VersionTuple);

fn version_to_py(version: Version) -> VersionTuple {
    (
        version.version_id,
        version.modifications_count,
        version.created_timestamp,
        version.modified_timestamp,
        version.session_id,
        version.client_identity,
    )
}

fn record_to_py(record: GetResult) -> Record {
    (
        record.key,
        record.value.map(|value| value.to_vec()),
        version_to_py(record.version),
    )
}

/// The comparison ordinals of `oxia.ComparisonType`.
fn comparison(value: u8) -> PyResult<ComparisonType> {
    match value {
        0 => Ok(ComparisonType::Equal),
        1 => Ok(ComparisonType::Floor),
        2 => Ok(ComparisonType::Ceiling),
        3 => Ok(ComparisonType::Lower),
        4 => Ok(ComparisonType::Higher),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown comparison type {other}"
        ))),
    }
}

/// An open client, on its own thread.
///
/// Every method blocks until the client's thread answers, with the GIL
/// released, so operations submitted from several Python threads run
/// concurrently and batch together.
#[pyclass(module = "oxia._oxia")]
struct Client {
    worker: Arc<Worker>,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (service_address, namespace, session_timeout_ms, client_identifier=None))]
    fn new(
        py: Python<'_>,
        service_address: String,
        namespace: String,
        session_timeout_ms: u64,
        client_identifier: Option<String>,
    ) -> PyResult<Self> {
        let mut builder = OxiaClientBuilder::new()
            .service_address(service_address)
            .namespace(namespace)
            .session_timeout(Duration::from_millis(session_timeout_ms));
        if let Some(identity) = client_identifier {
            builder = builder.identity(identity);
        }
        let worker = py
            .detach(|| Worker::start(builder))
            .map_err(|err| to_py_err(py, err))?;
        Ok(Client {
            worker: Arc::new(worker),
        })
    }

    #[pyo3(signature = (key, value, partition_key=None, expected_version_id=None, ephemeral=false, sequence_keys_deltas=None, secondary_indexes=None))]
    #[allow(clippy::too_many_arguments)]
    fn put(
        &self,
        py: Python<'_>,
        key: String,
        value: Vec<u8>,
        partition_key: Option<String>,
        expected_version_id: Option<i64>,
        ephemeral: bool,
        sequence_keys_deltas: Option<Vec<u64>>,
        secondary_indexes: Option<Vec<(String, String)>>,
    ) -> PyResult<(String, VersionTuple)> {
        let result = py.detach(|| {
            self.worker.run(move |client| async move {
                let mut put = client.put(key, value);
                if let Some(partition_key) = partition_key {
                    put = put.partition_key(partition_key);
                }
                if let Some(expected_version_id) = expected_version_id {
                    put = put.expected_version_id(expected_version_id);
                }
                if ephemeral {
                    put = put.ephemeral();
                }
                if let Some(deltas) = sequence_keys_deltas {
                    put = put.sequence_key_deltas(deltas);
                }
                for (name, value) in secondary_indexes.into_iter().flatten() {
                    put = put.secondary_index(name, value);
                }
                put.await
            })
        });
        let result = result.map_err(|err| to_py_err(py, err))?;
        Ok((result.key, version_to_py(result.version)))
    }

    #[pyo3(signature = (key, partition_key=None, comparison_type=0, include_value=true, use_index=None))]
    fn get(
        &self,
        py: Python<'_>,
        key: String,
        partition_key: Option<String>,
        comparison_type: u8,
        include_value: bool,
        use_index: Option<String>,
    ) -> PyResult<Record> {
        let comparison = comparison(comparison_type)?;
        let result = py.detach(|| {
            self.worker.run(move |client| async move {
                let mut get = client.get(key).comparison(comparison);
                if let Some(partition_key) = partition_key {
                    get = get.partition_key(partition_key);
                }
                if !include_value {
                    get = get.include_value(false);
                }
                if let Some(index) = use_index {
                    get = get.use_index(index);
                }
                get.await
            })
        });
        Ok(record_to_py(result.map_err(|err| to_py_err(py, err))?))
    }

    /// `False` when there was no such record, which is not an error here.
    #[pyo3(signature = (key, partition_key=None, expected_version_id=None))]
    fn delete(
        &self,
        py: Python<'_>,
        key: String,
        partition_key: Option<String>,
        expected_version_id: Option<i64>,
    ) -> PyResult<bool> {
        let result = py.detach(|| {
            self.worker.run(move |client| async move {
                let mut delete = client.delete(key);
                if let Some(partition_key) = partition_key {
                    delete = delete.partition_key(partition_key);
                }
                if let Some(expected_version_id) = expected_version_id {
                    delete = delete.expected_version_id(expected_version_id);
                }
                delete.await
            })
        });
        match result {
            Ok(()) => Ok(true),
            Err(oxia::OxiaError::KeyNotFound) => Ok(false),
            Err(err) => Err(to_py_err(py, err)),
        }
    }

    #[pyo3(signature = (min_key_inclusive, max_key_exclusive, partition_key=None))]
    fn delete_range(
        &self,
        py: Python<'_>,
        min_key_inclusive: String,
        max_key_exclusive: String,
        partition_key: Option<String>,
    ) -> PyResult<()> {
        py.detach(|| {
            self.worker.run(move |client| async move {
                let mut delete = client.delete_range(min_key_inclusive, max_key_exclusive);
                if let Some(partition_key) = partition_key {
                    delete = delete.partition_key(partition_key);
                }
                delete.await
            })
        })
        .map_err(|err| to_py_err(py, err))
    }

    #[pyo3(signature = (min_key_inclusive, max_key_exclusive, partition_key=None, use_index=None))]
    fn list(
        &self,
        py: Python<'_>,
        min_key_inclusive: String,
        max_key_exclusive: String,
        partition_key: Option<String>,
        use_index: Option<String>,
    ) -> PyResult<Vec<String>> {
        py.detach(|| {
            self.worker.run(move |client| async move {
                let mut list = client.list(min_key_inclusive, max_key_exclusive);
                if let Some(partition_key) = partition_key {
                    list = list.partition_key(partition_key);
                }
                if let Some(index) = use_index {
                    list = list.use_index(index);
                }
                list.await
            })
        })
        .map_err(|err| to_py_err(py, err))
    }

    #[pyo3(signature = (min_key_inclusive, max_key_exclusive, partition_key=None, use_index=None))]
    fn range_scan(
        &self,
        py: Python<'_>,
        min_key_inclusive: String,
        max_key_exclusive: String,
        partition_key: Option<String>,
        use_index: Option<String>,
    ) -> PyResult<RangeScanIterator> {
        let subscription = py.detach(|| {
            self.worker.open_stream(move |client| async move {
                let mut scan = client.range_scan(min_key_inclusive, max_key_exclusive);
                if let Some(partition_key) = partition_key {
                    scan = scan.partition_key(partition_key);
                }
                if let Some(index) = use_index {
                    scan = scan.use_index(index);
                }
                scan.stream().await
            })
        });
        Ok(RangeScanIterator::new(
            subscription.map_err(|err| to_py_err(py, err))?,
        ))
    }

    fn notifications(&self, py: Python<'_>) -> PyResult<NotificationIterator> {
        let subscription = py.detach(|| {
            self.worker.open_stream(move |client| async move {
                client.notifications().await.map(|n| n.map(Ok))
            })
        });
        Ok(NotificationIterator::new(
            subscription.map_err(|err| to_py_err(py, err))?,
        ))
    }

    fn sequence_updates(
        &self,
        py: Python<'_>,
        prefix_key: String,
        partition_key: String,
    ) -> PyResult<SequenceUpdatesIterator> {
        let subscription = py.detach(|| {
            self.worker.open_stream(move |client| async move {
                client
                    .sequence_updates(prefix_key, partition_key)
                    .await
                    .map(|u| u.map(Ok))
            })
        });
        Ok(SequenceUpdatesIterator::new(
            subscription.map_err(|err| to_py_err(py, err))?,
        ))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.worker.close())
            .map_err(|err| to_py_err(py, err))
    }
}

#[pymodule]
fn _oxia(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Client>()?;
    module.add_class::<RangeScanIterator>()?;
    module.add_class::<NotificationIterator>()?;
    module.add_class::<SequenceUpdatesIterator>()?;
    Ok(())
}
