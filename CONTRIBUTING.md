# Contributing to Oxia

Thank you so much for contributing to Oxia. We appreciate your time and help.
Here are some guidelines to help you get started.

## Code of Conduct

Be kind and respectful to the members of the community. Take time to educate
others who are seeking help. Harassment of any kind will not be tolerated.

## Questions

If you have questions regarding Oxia, feel free to start a new discussion: https://github.com/oxia-db/oxia/discussions/new/choose

## Filing a bug or feature

1. Before filing an issue, please check the existing issues to see if a
   similar one was already opened. If there is one already opened, feel free
   to comment on it.
1. If you believe you've found a bug, please provide detailed steps of
   reproduction, the version of Oxia and anything else you believe will be
   useful to help troubleshoot it (e.g. OS environment, configuration,
   etc...). Also state the current behavior vs. the expected behavior.
1. If you'd like to see a feature or an enhancement please create a new [idea
   discussion](https://github.com/oxia-db/oxia/discussions/new?category=ideas) with
   a clear title and description of what the feature is and why it would be
   beneficial to the project and its users.

## Development setup

Requirements:

* Python 3.10+
* [uv](https://docs.astral.sh/uv/) (package manager)
* A Rust toolchain, to build the extension module
* Docker (for integration tests via testcontainers)

The package is a Python layer over the [Oxia Rust
client](https://github.com/oxia-db/oxia-client-rust): `src/oxia` is the public
API and `rust/` is the PyO3 extension module it calls, built into the wheel by
[maturin](https://www.maturin.rs/).

Build the extension and install everything:

```bash
uv sync
```

`uv sync` installs the package in editable mode: changes under `src/oxia` take
effect right away. A change to the Rust side — `rust/`, `Cargo.toml` or
`Cargo.lock` — has to be recompiled with `uv sync --reinstall-package oxia`,
since a plain `uv sync` can reuse the build it has cached.

## Running tests

The suite is integration-level: every test runs against a standalone Oxia
server in a container, so Docker has to be running.

```bash
# The whole suite
uv run pytest

# With coverage
uv run pytest --cov --cov-report=term-missing

# The Rust side
cargo fmt --check && cargo clippy --all-targets -- -D warnings
```

## Building the documentation

```bash
uv run pydoctor --config pydoctor.ini
```

## Submitting changes

1. Fork the project.
1. Clone your fork (`git clone https://github.com/your_username/oxia-client-python && cd oxia-client-python`)
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Make changes and run tests (`uv run pytest`)
1. Commit your changes with DCO sign-off (`git commit -s -m 'Add some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create a new pull request

**Note:** All commits must include a
[DCO sign-off](https://developercertificate.org/) (`git commit -s`).
