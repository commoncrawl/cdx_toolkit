'''
Range reads from S3 (aioboto3).
'''
from cdx_toolkit.repackage.jobs import RangeJob
from cdx_toolkit.repackage.readers import RangeReader
from cdx_toolkit.repackage.readers.http import RangeError
from cdx_toolkit.repackage.retries import RETRYABLE_ERRORS, with_retries
from cdx_toolkit.repackage.urls import parse_s3_uri


class S3RangeReader(RangeReader):
    """Ranged get_object requests with an aioboto3 S3 client."""

    def __init__(self, client, max_attempts: int, base_backoff_seconds: float):
        self.client = client
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds

    async def read(self, job: RangeJob) -> bytes:
        bucket, key = parse_s3_uri(job.url)
        end = job.offset + job.length - 1  # inclusive

        async def transfer() -> bytes:
            """The whole transfer is retried: the request and reading the body (closed in any case)."""
            resp = await self.client.get_object(Bucket=bucket, Key=key, Range=f'bytes={job.offset}-{end}')
            async with resp['Body'] as body:
                data = await body.read()
            if len(data) != job.length:
                raise RangeError(f'{job.url}: requested {job.length} bytes from offset {job.offset}, got {len(data)}')
            return data

        return await with_retries(
            transfer,
            op_name=f'ranged_get {bucket}/{key}[{job.offset}:{end}]',
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
            retry_on=RETRYABLE_ERRORS + (RangeError,),
        )
