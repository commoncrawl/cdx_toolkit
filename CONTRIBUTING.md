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

By default, all remote requests are mocked: the CDX API and HTTP downloads are served from recorded data in `tests/data`, S3 is a local [moto](https://github.com/getmoto/moto) server (installed with `.[all]`; without it, the S3 tests are skipped), and the Hugging Face reads are faked. The mocked requests still wait for the per-host rate limits ([docs/settings.md](docs/settings.md)); the CI shortens them:

```bash
export CDXT_DEFAULT_MIN_RETRY_INTERVAL=0.01 CDXT_CC_INDEX_MIN_RETRY_INTERVAL=0.01 \
       CDXT_CC_DATA_MIN_RETRY_INTERVAL=0.01 CDXT_IA_MIN_RETRY_INTERVAL=0.01
```

To test services live instead, set `CDXT_LIVE_TESTS` to a comma-separated list of them, or `all` (unset the short intervals above, the live servers rate-limit):

| Service | Live tests |
|---|---|
| `http` | the CDX API (index.commoncrawl.org, web.archive.org) and HTTP downloads (data.commoncrawl.org); needs an allowed IP address |
| `s3` | the S3 tests against AWS, and DuckDB queries of Common Crawl's index on S3; needs AWS credentials |
| `athena` | the Athena queries; needs AWS credentials with Athena access |
| `hf` | `cdxt index cc-url-index-hf`, and `cdxt repackage` reading from `hf://buckets/commoncrawl/commoncrawl` and writing to `hf://buckets/commoncrawl/commoncrawl-ci-temp` (`CDXT_TEST_HF_BUCKET`); writing needs a Hugging Face token (`HF_TOKEN` or `hf auth login`) |

```bash
CDXT_LIVE_TESTS=s3,athena,hf make test
```

The CI workflow of pull requests runs the mocked tests only; the `CI-slow` workflow (started manually) runs the live tests with AWS credentials.

The WARC records that the `cdxt repackage` tests download (from `data.commoncrawl.org` or `s3://commoncrawl`) are recorded in `tests/data/repackage/warc_records.jsonl`, and DuckDB queries a small local index built from the same captures (the `local_url_index` fixture in `tests/conftest.py`).

If the remote APIs change, new mock data can be semi-automatically collected by setting another environment variable,  running corresponding unit tests, and overwriting existing mock data in `tests/data/mock_responses`:

```bash
# set environment variable (CDXT_LIVE_TESTS should not have http)
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
