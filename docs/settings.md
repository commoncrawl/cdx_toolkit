# Settings

## S3 settings

For requests to S3 (e.g. `s3://commoncrawl`), `cdxt` uses Common Crawl's
recommended AWS settings: region `us-east-1`, `retry_mode = adaptive`,
`max_attempts = 100000`, and for S3 `addressing_style = virtual`,
`payload_signing_enabled = false` and `max_concurrent_requests = 2`.
Settings you configured yourself win: environment variables (`AWS_REGION`,
`AWS_DEFAULT_REGION`, `AWS_RETRY_MODE`, `AWS_MAX_ATTEMPTS`), the AWS config file
of your profile (`~/.aws/config`, `AWS_PROFILE`), and options in the fsspec
configuration. `CDXT_AWS_DEFAULTS=0` turns the defaults off.

The Athena client uses the same retry settings (its region is part of the
`athena://` location). DuckDB has no adaptive retry mode and no limit for
concurrent requests; for S3 it uses the region (`s3_region`), the addressing
style (`s3_url_style`) and `max_attempts` (`http_retries`, with 1 second between
retries).

## HTTP requests: rate limits and retries

Requests over HTTP(S) go through `cdx_toolkit/myrequests.py`:

- CDX API queries: `index.commoncrawl.org` for `cc` (and its list of crawls, `collinfo.json`, which is
  cached for a day in `CDXT_CACHE_DIR`, default `~/.cache/cdx_toolkit/`), `web.archive.org/cdx/search/cdx`
  for `ia`, and other CDX API servers;
- WARC records read over HTTP(S), e.g. from `https://data.commoncrawl.org` (`cdxt repackage` with the
  default `--warc-download-prefix`, and `capture.content` in Python);
- wayback replays, e.g. from `https://web.archive.org/web` (`cdxt repackage --wb ia`).

Reads from `s3://` (see above), `hf://`, and the queries of DuckDB and Athena do not use it.

### Minimum interval between requests

Requests to the same host start at least a minimum interval apart, also when they are made from several
threads (e.g. `cdxt repackage --parallel-readers`). After a request, including its retries, the next one
waits the interval again.

| Host | Interval | Environment variable |
|---|---|---|
| `index.commoncrawl.org` | 1.0 s | `CDXT_CC_INDEX_MIN_RETRY_INTERVAL` |
| `data.commoncrawl.org` | 0.55 s | `CDXT_CC_DATA_MIN_RETRY_INTERVAL` |
| `web.archive.org` (CDX API and replays) | 6.0 s | `CDXT_IA_MIN_RETRY_INTERVAL` |
| any other host | 3.0 s | `CDXT_DEFAULT_MIN_RETRY_INTERVAL` |

The interval holds within a process: `cdxt repackage --processes N` makes up to N times as many requests
to a host. So reads from `https://data.commoncrawl.org` are limited to at most about 1.8 requests per second
per process, whatever `--parallel-readers` is; for faster reads, use `s3://commoncrawl` in AWS us-east-1.
`--wb` cannot be used with `--processes` for this reason. Please do not lower the intervals for Common
Crawl's and the Internet Archive's servers: they are shared by everybody.

### Retries

| Response | What happens |
|---|---|
| status 429, 500, 502, 503, 504, 509 (slow down, temporary errors) | retried without a limit; the wait starts at twice the host's interval and doubles up to 60 s; logged as a warning from the 6th retry |
| connection error, timeout (30 s to connect, 30 s between bytes), broken response | retried after 60 s; a warning from the 11th error (`CDXT_WARNING_AFTER_N_ERRORS`, default 10), fails after 101 errors (`CDXT_MAX_ERRORS`, default 100) |
| unknown hostname (DNS error) | fails at once, unless the host answered before in this process (or is `data.commoncrawl.org`, `commoncrawl.s3.amazonaws.com` or `web.archive.org`): then it is retried like a connection error |
| status 400 or 404 from a CDX API | not an error: no captures, or past the last page |
| status 404 when replaying a capture with status 404, or a revisit (`-`) | not an error: the replay is the 404 |
| other status 400 or 404, and other 4xx/5xx statuses | fails at once, no retry |

Redirects are not followed. Requests send the `User-Agent` `pypi_cdx_toolkit/<version>`.

In `cdxt repackage`, a capture whose request fails is counted as failed (the other captures are still
written, see the README). When the run is stopped, the waits end at once; a request in progress ends with
its timeout.
