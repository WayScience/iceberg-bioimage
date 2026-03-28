# Contributing

This project uses `uv`, `pytest`, `ruff`, and Sphinx. Contributions should
keep the core package engine-neutral and preserve the separation between:

- scanning and canonicalization
- validation and publishing
- optional query integrations

## Development setup

1. Install Python 3.11 or newer.
2. Install `uv`: https://docs.astral.sh/uv/getting-started/installation/
3. Sync the default development environment:

```bash
uv sync --group dev --group docs
```

If you want to work on the optional DuckDB helpers too:

```bash
uv sync --group dev --group docs --group duckdb
```

## Local checks

Run the same checks that are expected in CI:

```bash
uv run pytest
uv run ruff check
uv run --group docs sphinx-build -b html docs/src docs/_build/html
```

Optional DuckDB integration checks:

```bash
uv run --group duckdb pytest tests/test_duckdb.py
```

If you use `pre-commit`, install the hooks and run them locally:

```bash
pre-commit install
pre-commit run --all-files
```

## Pull requests

Keep pull requests narrow and technically coherent. Small changes review much
faster than broad, mixed-scope updates.

Before opening a pull request:

1. Run the local checks above.
2. Update docs and examples for any user-facing behavior changes.
3. Add or update tests when behavior changes.
4. Follow the pull request template in [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md).

## Releases

Releases are published from GitHub Releases and PyPI trusted publishing.
Release notes are drafted automatically with Release Drafter.

The expected release flow is:

1. Merge reviewed changes to `main`.
2. Confirm CI is green.
3. Review the drafted GitHub release.
4. Publish the release from GitHub.
5. Let the PyPI publish workflow build and upload the package.

## Code of conduct

This project is governed by [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
