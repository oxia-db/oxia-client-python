
<h2 align="center">Oxia Client SDK for Python</h1>
a robust, scalable metadata store and coordination system designed for large-scale distributed systems, with built-in support for stream index storage to optimize real-time data management.

<p align="center">
  <a href="https://oxia-db.github.io/docs/getting-started">Getting Started </a> | <a href="https://oxia-db.github.io/">Documentation</a>
</p>

<p align="center">
  <a href="https://github.com/oxia-db/oxia-client-python/releases"><img src="https://img.shields.io/github/v/release/oxia-db/oxia" alt="Latest Release"></a>
  <a href="https://github.com/oxia-db/oxia-client-python/actions/workflows/run-tests.yaml/badge.svg"><img src="https://github.com/oxia-db/oxia-client-python/actions/workflows/run-tests.yaml/badge.svg" alt="CI"></a>
  <a href="https://github.com/oxia-db/oxia-client-python/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-white.svg" alt="License"></a>
  <a href="https://github.com/oxia-db/oxia-client-python/discussions/new/choose"><img src="https://img.shields.io/badge/Github-Discussion-blue.svg?logo=refinedgithub" alt="Github Discussion"></a>
</p>

<br>

### Installing

```bash
pip install oxia
```

```python
import oxia

client = oxia.Client('localhost:6648', namespace='default')
key, version = client.put('my-key', 'my-value')
key, value, version = client.get('my-key')
client.close()
```

### How it works

This SDK is a binding over the [Oxia Rust
client](https://github.com/oxia-db/oxia-client-rust), through
[PyO3](https://pyo3.rs/): the wheel carries that client compiled in, and the
Python package is the API over it. The client runs on a thread of its own, and
operations from several Python threads run concurrently and batch together —
calls release the GIL while they wait.

Wheels are published for Linux (x86\_64, aarch64) and macOS (x86\_64,
aarch64), and each serves every CPython from 3.13 on. Installing from the
source distribution instead requires a Rust toolchain.

<br><br>

### Contributing to Oxia

Please 🌟 star the project if you like it. 

Feel free to open an [issue](https://github.com/oxia-db/oxia-client-python/issues/new) or start a [discussion](https://github.com/oxia-db/oxia/discussions/new/choose). You can also follow the development [guide]() to contribute and build on it.

### License

Copyright 2025 The Oxia Authors

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0