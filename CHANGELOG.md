- unreleased
	+ new command line interface: `cdxt index` and `cdxt repackage` (see README)
	+ `cdxt iter` is a deprecated alias of `cdxt index` (same arguments, prints a deprecation warning)
	+ removed `cdxt size` and `cdxt warc`; they print the equivalent new command
	+ `cdxt index`: `--hostname`, `--domain`, repeatable `--url`, `-o/--output` (local or remote),
	  JSONL/CDXJ/CSV/TSV output, all fields by default, `--count` replaces `size`
	+ `cdxt repackage`: reads captures from a file or stdin, human-readable `--size`, `--is-part-of`, `--description`
	+ `--cc-sort` is now passed on to the CDX fetcher
	+ the log level variable is `CDXT_LOGLEVEL` (`LOGLEVEL` still works), like cdx_toolkit's other environment variables
	+ `cdxt index` queries Common Crawl's columnar URL index with DuckDB (`cc-url-index-s3`, `cc-url-index-hf`, `*.parquet`)
	  or AWS Athena (`athena://...`): `--hostname`/`--domain`/`--crawl`/`--limit` or your own `--sql`, cost confirmation (`--yes`), `--dry-run`
	+ `cdxt repackage` copies WARC records byte-for-byte with the async WARC fetcher (from the feat/warc-range-sources branch):
	  reads from https://, s3:// and hf:// (Hugging Face Storage Buckets), writes to local, s3:// or hf://,
	  `--processes`, `--parallel-readers`, `--add-metadata-record`, glob input; output files are now named `<prefix>-001.warc.gz`
	  (`--wb` wayback replays too, which now work with `--parallel-readers` and `--add-metadata-record`); with
	  `--processes`, the processes share one file counter
	  and write the final file names directly, so each `warcinfo` record names its own file
	+ `hf://` WARC reads resolve each WARC file once and read its records from the signed CDN URL
	  (one HF Resolvers rate-limit request per WARC file instead of per record)
	+ `cdxt index` reads CDX files (local or remote, globs): `--url`/`--hostname`/`--domain`/`--url-file` matched as SURT
	  prefixes (`--url-file-type domain` for host lists), one output per CDX file for an `-o` path without extension,
	  `--processes`
	+ SQL indexes: `--url` patterns (matched against `url_surtkey`), `--from`/`--to` (fetch time), `--language` (primary content language, three-letter ISO 639-3 codes)
	+ `--output-format cdx` / `--index-format cdx` as another name for `cdxj`; clear error for globs over http(s)
	+ Parquet output for `cdxt index` and Parquet input for `cdxt repackage` (`cdx_toolkit[parquet]`)
	+ `cdxt repackage`: default `--prefix REPACKAGE` (was `TEST`), no WARC file is written without captures,
	  a full URL in `warc_filename`/`filename` is used as it is (the `warc_url` column is gone); stops (exit status 2)
	  if WARC files with the same prefix exist already, instead of overwriting them (also with `--wb`, which
	  numbered its files after the existing ones before)
	+ fixed `--to` with a day (e.g. `--to 20240410`), which was padded to the end of the month, and February in leap years
	+ fixed DNS errors of hosts that worked before: they are retried again (like connection errors) instead of failing at once
	+ removed the deprecated `cdx_iter` and `cdx_size` scripts
	+ S3 requests use Common Crawl's recommended AWS settings (us-east-1, adaptive retries, 100000 attempts, virtual
	  addressing, no payload signing, 2 concurrent requests) unless configured by the user; `CDXT_AWS_DEFAULTS=0` turns them off;
	  also for Athena and, as far as DuckDB has equivalents, for DuckDB (fixes DuckDB's `http_timeout`, which is in seconds)
	+ `cdxt repackage --wb` checks the captures before replaying them: `url` (http/https), `timestamp` and `status`
	  are needed, and Common Crawl captures are refused (as `cdxt --cc warc` refused `--wb`)
	+ Python API of the new commands: `cdx_toolkit.indexes.open_index()` with `IndexQuery` (`records()`, `count()`, `explain()`)
	  and `cdx_toolkit.repackage.repackage()`; `cdxt index` and `cdxt repackage` run the same code
	+ deprecated `cdx_toolkit.warc.get_writer()` and `CDXToolkitWARCWriter` (FutureWarning): use `repackage()`
	+ new extras: `athena`, `duckdb`, `hf`, `parquet`; `fsspec` and `aiofiles` are now required

- 0.9.38
	+ deprecated support for py3.7 and py.3.8
	+ added support for py3.13 and py3.14
	+ fixed deprecation warning from pkg_resources
	+ refactored CI to use mocked remote requests

- 0.9.37
	+ --crawl for CCF

- 0.9.36
	+ ratelimit code; both IA and CCF are rate limiting their cdx endpoints
	+ cache collinfo.json in ~/.cache/cdx_toolkit/
	+ py3.11 and py3.12 pass testing; windows and macos pass testing

- 0.9.35
    + exponential backoff retries now that IA is sending 429

- 0.9.34
	+ renamed class CDXFetcher kwarg warc_url_prefix to warc_download_prefix
	+ changed CC warc downloads to use the Cloudfront url, which has rate limits
	+ made 429 and 509 retryable errors. Neither IA or CC sends this status.

- 0.9.33
	+ rename master to main
	+ drop python 3.5 testing because of setuptools-scm

- 0.9.32
	+ there was no 0.9.32

- 0.9.31
	+ pywb 2.5 changed a json error message key
	+ tolerate the first capture returned for example.com being a revisit record

- 0.9.30
	+ add support for CC early indices, 2008-2010
	+ MacOS support marked in setup.py

- 0.9.29
	+ python 3.9 support marked in setup.py

- 0.9.28
	+ expose warc_version= keyword argument for warc writing (but it's untested and broken for --ia warc)
	+ improve dns retry algorithm: always retry for hostnames we expect to exist (ia, cc)

- 0.9.27
	+ packaging: fix for using markdown without explicit conversion in setup.py

- 0.9.26
	+ 10x effort for cdx server timeouts, but fail immediately for dns failures
	+ give a human-useful error if the user passes in a unix timestamp instead of a cdx timestamp

- 0.9.25
	+ allow multiple --filter args in the cli, and filter=list in the api
	+ drop python 3.4 support because requests 2.22 no longer supports it
	+ python 3.8 works

- 0.9.24
	+ rename cdx.items() to cdx.iter() with a deprecation warning
	+ use warcio for all warc reading/writing
	+ test coverage 99%

- 0.9.23
	+ add 'cdxt' command-line tool
	+ deprecate cdx_iter and cdx_size command-line scripts
	+ migrate tests to use 'cdxt' with much better error-checking
	+ add warc 'subprefix' to warcinfo isPartOf line
	+ made default limit=1000 apply only to get, not iter
	+ make iterator results be delivered incrementally
	+ start changelog

