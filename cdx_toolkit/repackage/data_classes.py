import asyncio
import time
from dataclasses import dataclass

from cdx_toolkit.repackage.s3_utils import RETRYABLE_ERRORS, is_s3_url, parse_s3_uri, with_retries
from cdx_toolkit.repackage.hf_utils import is_hf_url
from typing import Optional, Tuple

from cdx_toolkit.myrequests import myrequests_get
from cdx_toolkit.repackage.http_range import RangeError, check_range_response


@dataclass
class ThroughputTracker:
    """Track throughput metrics for fetchers and consumers."""

    start_time: float = 0.0
    total_bytes: int = 0
    total_requests: int = 0
    total_records: int = 0

    def start(self):
        self.start_time = time.time()

    def add(self, bytes_count: int = 0, records_count: int = 0, requests_count: int = 1):
        self.total_bytes += bytes_count
        self.total_requests += requests_count
        self.total_records += records_count

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time

        return {
            'elapsed': elapsed,
            'total_bytes': self.total_bytes,
            'total_requests': self.total_requests,
            'total_records': self.total_records,
            'bytes_per_sec': self.total_bytes / elapsed if elapsed > 0 else 0,
            'mb_per_sec': (self.total_bytes / elapsed) / (1024 * 1024) if elapsed > 0 else 0,
            'requests_per_sec': self.total_requests / elapsed if elapsed > 0 else 0,
            'records_per_sec': self.total_records / elapsed if elapsed > 0 else 0,
        }


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
            # the minimum interval between requests to a host across threads
            resp = await asyncio.to_thread(myrequests_get, self.url, headers=headers)
            check_range_response(
                resp.status_code, resp.headers.get('Content-Range'), len(resp.content), offset, length, self.url
            )
            return resp.content


@dataclass(frozen=True)
class RangePayload:
    """Bytes output from S3 or HTTP range read."""

    job: RangeJob
    data: bytes
