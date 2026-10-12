import time
from dataclasses import dataclass, field

from cdx_toolkit.repackage.s3_utils import is_s3_url, parse_s3_uri, with_retries
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
    # Relative WARC filename (e.g. crawl-data/...warc.gz) as known by the source,
    # used when materializing a non-self-contained range-jobs CSV. `url` stays
    # authoritative for fetching.
    filename: Optional[str] = None
    # Extra columns from a raw SQL --query (e.g. content_languages), kept only for
    # CSV materialization / analysis. Never used for fetching. Excluded from
    # equality/hash so RangeJob stays hashable despite the dict value.
    extra: Optional[dict] = field(default=None, compare=False)

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
        s3_client=None,
        hf_reader=None,
    ) -> bytes:
        """Ranged get request to S3 (aioboto3), an HF bucket, or HTTP."""
        offset = self.offset
        length = self.length

        end = offset + length - 1  # inclusive

        if self.is_s3():
            # read from S3
            bucket, key = self.get_s3_bucket_and_key()
            resp = await with_retries(
                lambda: s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes={offset}-{end}'),
                op_name=f'ranged_get {bucket}/{key}[{offset}:{end}]',
                max_attempts=max_attempts,
                base_backoff_seconds=base_backoff_seconds,
            )
            data = await resp['Body'].read()
            if len(data) != length:
                raise RangeError(f'{self.url}: requested {length} bytes from offset {offset}, got {len(data)}')
            return data

        elif self.is_hf():
            # read from a Hugging Face bucket (hf://) via the configured HF reader
            if hf_reader is None:
                raise ValueError('hf:// range read requires an hf_reader (none was provided)')
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

            resp = myrequests_get(self.url, headers=headers)
            check_range_response(
                resp.status_code, resp.headers.get('Content-Range'), len(resp.content), offset, length, self.url
            )
            return resp.content


@dataclass(frozen=True)
class RangePayload:
    """Bytes output from S3 or HTTP range read."""

    job: RangeJob
    data: bytes
