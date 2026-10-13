'''
The captures to repackage: index records (as written by `cdxt index`) -> range jobs, and the result of a run.
'''
import logging
import threading
from dataclasses import dataclass
from typing import Iterable, Iterator, Mapping, Optional, Tuple

from cdx_toolkit.myrequests import myrequests_get
from cdx_toolkit.repackage.readers.http import RangeError, check_range_response, run_in_daemon_thread
from cdx_toolkit.repackage.retries import RETRYABLE_ERRORS, with_retries
from cdx_toolkit.repackage.urls import is_hf_url, is_s3_url, parse_s3_uri


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepackageResult:
    """Number of captures to repackage, written to WARC files, and failed (not fetched)."""

    requested: int = 0
    written: int = 0
    failed: int = 0

    def __add__(self, other: 'RepackageResult') -> 'RepackageResult':
        return RepackageResult(
            self.requested + other.requested, self.written + other.written, self.failed + other.failed
        )


@dataclass(frozen=True)
class RangeJob:
    """Defines a S3 or HTTP range read request."""

    url: str
    offset: int
    length: int
    records_count: int = 1
    # WARC filename of the record as given in the index (e.g. crawl-data/...warc.gz, relative to the download
    # prefix), used to sort and shard the jobs by WARC file; `url` is authoritative for fetching.
    filename: Optional[str] = None

    def is_s3(self):
        return is_s3_url(self.url)

    def get_s3_bucket_and_key(self) -> Tuple[str, str]:
        if self.is_s3():
            return parse_s3_uri(self.url)
        else:
            raise ValueError('Cannot get bucket and key from a HTTP job')

    def is_hf(self):
        return is_hf_url(self.url)

    async def ranged_get_bytes(
        self,
        max_attempts: int,
        base_backoff_seconds: float,
        readers=None,
    ) -> bytes:
        """Ranged get request to S3 (aioboto3), an HF bucket, or HTTP; `readers` provides the S3 and HF readers."""
        offset = self.offset
        length = self.length

        end = offset + length - 1  # inclusive

        if self.is_s3():
            # read from S3
            bucket, key = self.get_s3_bucket_and_key()
            if readers is None:
                raise ValueError('s3:// range read requires readers (none were provided)')
            s3_client = await readers.s3()

            async def transfer() -> bytes:
                """The whole transfer is retried: the request and reading the body (closed in any case)."""
                resp = await s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes={offset}-{end}')
                async with resp['Body'] as body:
                    data = await body.read()
                if len(data) != length:
                    raise RangeError(f'{self.url}: requested {length} bytes from offset {offset}, got {len(data)}')
                return data

            return await with_retries(
                transfer,
                op_name=f'ranged_get {bucket}/{key}[{offset}:{end}]',
                max_attempts=max_attempts,
                base_backoff_seconds=base_backoff_seconds,
                retry_on=RETRYABLE_ERRORS + (RangeError,),
            )

        elif self.is_hf():
            # read from a Hugging Face bucket (hf://) via the configured HF reader
            if readers is None:
                raise ValueError('hf:// range read requires readers (none were provided)')
            hf_reader = await readers.hf()
            return await hf_reader.ranged_get(
                self.url,
                offset,
                length,
                max_attempts=max_attempts,
                base_backoff_seconds=base_backoff_seconds,
            )

        else:
            # read from HTTP
            headers = {'Range': 'bytes={}-{}'.format(offset, end)}

            # in a thread, so that the event loop (other readers, the writer) is not blocked; myrequests_get keeps
            # the minimum interval between requests to a host across threads. If this task is cancelled, the
            # thread stops at its next wait (before a retry), and nothing waits for it.
            stop = threading.Event()
            try:
                resp = await run_in_daemon_thread(myrequests_get, self.url, headers=headers, stop=stop)
            finally:
                stop.set()
            check_range_response(
                resp.status_code, resp.headers.get('Content-Range'), len(resp.content), offset, length, self.url
            )
            return resp.content


@dataclass(frozen=True)
class RangePayload:
    """Bytes output from S3 or HTTP range read."""

    job: RangeJob
    data: bytes


WARC_LOCATION_FIELDS = ('filename', 'offset', 'length')


class MissingWarcLocation(ValueError):
    """A record has no WARC location (filename, offset, length), e.g. an Internet Archive capture."""


def join_warc_url(prefix: Optional[str], warc_filename: str) -> str:
    """Join a download prefix with a warc_filename robustly.

    - if warc_filename is already absolute (contains '://'), return it unchanged
      (supports custom queries / self-contained index files whose value is a full URL);
    - if prefix is empty/None, return warc_filename unchanged;
    - otherwise join with exactly one '/' (no double slash, no missing slash)."""
    if '://' in warc_filename:
        return warc_filename
    if not prefix:
        return warc_filename
    return prefix.rstrip('/') + '/' + warc_filename.lstrip('/')


class RecordsSource:
    """RangeJobs from index records (as written by `cdxt index`) with filename, offset and length fields.

    With `sort` (the default), jobs are sorted by (WARC file, record offset) for better
    range-read locality. The records are then read (and validated) when the source is
    created, and kept in memory; pass sort=False to stream them in input order."""

    def __init__(self, records: Iterable[Mapping], warc_download_prefix: Optional[str], sort: bool = True):
        self.warc_download_prefix = warc_download_prefix
        self.jobs = (self.to_range_job(record) for record in records)
        if sort:
            self.jobs = sorted(self.jobs, key=lambda j: (j.filename or j.url, j.offset))

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        """Yield the range jobs; `url` is authoritative for fetching, `filename` is the filename of the record."""
        yield from self.jobs

    def to_range_job(self, record: Mapping) -> RangeJob:
        if not all(record.get(field) not in (None, '') for field in WARC_LOCATION_FIELDS):
            raise MissingWarcLocation(
                f'capture {record.get("url")} has no WARC location (filename, offset, length); '
                'use --wb to fetch it from a wayback machine'
            )
        return RangeJob(
            url=join_warc_url(self.warc_download_prefix, record['filename']),
            offset=int(record['offset']),
            length=int(record['length']),
            filename=record['filename'],
        )
