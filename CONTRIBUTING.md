# Contributing to cdx_toolkit

We welcome contributions to cdx_toolkit! Whether you're adding new features, improving documentation, or fixing bugs, your help is greatly appreciated.

## Local installation

Clone the repository, setup a virtual environment, and run the following command:

```bash
make install
```

For S3-related features or tests, install optional dependencies:

```bash
pip install -e ".[s3]"
```

To install everything (dev/test/S3), use:

```bash
pip install -e ".[all]"
```

## Tests

To test code changes, please run our test suite before submitting pull requests:

```bash
# all tests (same as CI)
make test

# simple unit tests without integration tests
make unit
```

By default, all remote requests are mocked: the CDX API and HTTP downloads are served from recorded data in `tests/data`, and S3 is a local [moto](https://github.com/getmoto/moto) server (installed with `.[all]`; without it, the S3 tests are skipped). To change this behaviour and actually call remote APIs (if you run this from a whitelisted IP address), the following environment variable can be set:

```bash
export CDXT_DISABLE_MOCK_RESPONSES=1
```

The S3 tests run against AWS, and the tests that need AWS and cannot be mocked (Athena, DuckDB queries of Common Crawl's index on S3) run at all, only with AWS credentials and:

```bash
export CDXT_LIVE_TESTS=1
```

The CI workflow of pull requests runs the mocked tests only; the `CI-slow` workflow (started manually) runs the live tests with AWS credentials.

The WARC records that the `cdxt repackage` tests download (from `data.commoncrawl.org` or `s3://commoncrawl`) are recorded in `tests/data/repackage/warc_records.jsonl`, and DuckDB queries a small local index built from the same captures (the `local_url_index` fixture in `tests/conftest.py`).

If the remote APIs change, new mock data can be semi-automatically collected by setting another environment variable,  running corresponding unit tests, and overwriting existing mock data in `tests/data/mock_responses`:

```bash
# set environment variable (CDXT_DISABLE_MOCK_RESPONSES should not be set)
export CDXT_SAVE_MOCK_RESPONSES=./tmp/mock_responses

# run the test for what mock data should be saved to $CDXT_SAVE_MOCK_RESPONSES/<test_file>/<test_func>.jsonl
pytest tests/test_cli.py::test_basics
```

## Code format & linting

Please following the definitions from `.editorconfig` and `.flake8`.

To test the linting, run this command:

```bash
make lint
```

To automatically run the linting on every commit, you can use the pre-commit hooks (automatically installed via `make install`).

You can also run the hooks manually on all files:

```bash
pre-commit run --all-files
```
