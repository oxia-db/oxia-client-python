# Oxia Python Client SDK

[![CI](https://github.com/oxia-db/oxia-client-python/actions/workflows/run-tests.yaml/badge.svg)](https://github.com/oxia-db/oxia-client-python/actions/workflows/run-tests.yaml)
[![License](https://img.shields.io/badge/license-Apache%202.0-white.svg)](https://github.com/oxia-db/oxia-client-python/blob/main/LICENSE)

Python client for [Oxia](https://oxia-db.github.io/), a scalable metadata store and coordination
system for large-scale distributed systems.

<p align="center">
  <a href="https://oxia-db.github.io/docs/getting-started">Getting Started</a> |
  <a href="https://oxia-db.github.io/">Documentation</a> |
  <a href="https://github.com/oxia-db/oxia/discussions/new/choose">Discussion</a>
</p>

## Requirements

* Python 3.10+

## Installation

```bash
pip install oxia
```

## Quick start

```python
import oxia

with oxia.Client('localhost:6648') as client:
    # Put a value
    key, version = client.put('/my-key', b'my-value')

    # Get it back
    key, value, version = client.get('/my-key')

    # Conditional put (only if version matches)
    key, version = client.put('/my-key', b'new-value',
                              expected_version_id=version.version_id())

    # List keys in a range
    keys = client.list('/my-', '/my-~')

    # Delete
    client.delete('/my-key')
```

## Features

- **CRUD operations**: `put`, `get`, `delete`, `delete_range`
- **Range queries**: `list` (keys only), `range_scan` (keys + values)
- **Conditional writes**: version-based optimistic concurrency via `expected_version_id`
- **Ephemeral records**: session-scoped keys that are removed when the client disconnects
- **Notifications**: subscribe to real-time change events
- **Sequential keys**: server-assigned monotonic key suffixes
- **Secondary indexes**: additional lookup keys on records
- **Partition routing**: co-locate related records on the same shard via `partition_key`
- **Floor/Ceiling/Lower/Higher lookups**: nearest-key queries

For the full API reference, see the [documentation](https://oxia-db.github.io/oxia-client-python/latest/).

## Contributing

Please star the project if you like it!

Feel free to open an [issue](https://github.com/oxia-db/oxia-client-python/issues/new)
or start a [discussion](https://github.com/oxia-db/oxia/discussions/new/choose).
See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Copyright 2025 The Oxia Authors

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
