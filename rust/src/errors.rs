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

//! Turns a native [`OxiaError`] into the matching `oxia.ex` exception.
//!
//! The exception classes stay in Python, in `oxia/ex.py`, so that they keep
//! their documentation and their identity for `except` clauses. This module
//! imports that module the first time an error has to be raised — never at
//! module initialization, which runs while `oxia/__init__.py` is still
//! executing — and caches it.

use oxia::OxiaError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;

static EX_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

/// The `oxia.ex` class an error is reported as.
///
/// The four semantic outcomes the Python API documents map to their own
/// exception; everything else — timeouts, disconnections, a closed client, a
/// gRPC status — is the base `OxiaException`, which is also what a caller
/// catching any of the others would catch.
fn class_name(err: &OxiaError) -> &'static str {
    match err {
        OxiaError::KeyNotFound => "KeyNotFound",
        OxiaError::UnexpectedVersionId => "UnexpectedVersionId",
        OxiaError::SessionExpired => "SessionNotFound",
        OxiaError::InvalidArgument(_) => "InvalidOptions",
        _ => "OxiaException",
    }
}

fn raise(py: Python<'_>, name: &str, message: String) -> PyResult<PyErr> {
    let module = EX_MODULE
        .get_or_try_init(py, || PyModule::import(py, "oxia.ex").map(|m| m.unbind()))?
        .bind(py);
    let class = module.getattr(name)?;
    Ok(PyErr::from_value(class.call1((message,))?))
}

/// Converts a native error into the Python exception to raise for it.
pub(crate) fn to_py_err(py: Python<'_>, err: OxiaError) -> PyErr {
    match raise(py, class_name(&err), err.to_string()) {
        Ok(err) => err,
        // `oxia.ex` could not be imported, or a class is missing from it: the
        // import error is the more useful one to surface.
        Err(err) => err,
    }
}
