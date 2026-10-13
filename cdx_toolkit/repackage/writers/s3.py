import logging
from typing import List, Dict, Optional

from cdx_toolkit.repackage.retries import with_retries

logger = logging.getLogger(__name__)


async def mpu_create(
    s3,
    bucket: str,
    key: str,
    *,
    max_attempts: int,
    base_backoff_seconds: float,
):
    """Create multi part upload to S3."""
    kwargs = {'Bucket': bucket, 'Key': key}
    resp = await with_retries(
        lambda: s3.create_multipart_upload(**kwargs),
        op_name=f'create_multipart_upload {bucket}/{key}',
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp['UploadId']


async def mpu_upload_part(
    s3,
    bucket: str,
    key: str,
    upload_id: str,
    part_number: int,
    body: bytes,
    max_attempts: int,
    base_backoff_seconds: float,
) -> str:
    """Upload a part of a multi-part upload to S3."""
    resp = await with_retries(
        lambda: s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        ),
        op_name=f'upload_part {bucket}/{key}#{part_number}',
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp['ETag']


async def mpu_complete(
    s3,
    bucket: str,
    key: str,
    upload_id: str,
    parts: List[Dict],
    max_attempts: int,
    base_backoff_seconds: float,
):
    """Send complete for multi-part upload."""
    await with_retries(
        lambda: s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts}
        ),
        op_name=f'complete_multipart_upload {bucket}/{key}',
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )


async def mpu_abort(s3, bucket: str, key: str, upload_id: str):
    """Abort mult-part upload."""
    try:
        await s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    except Exception:
        logger.exception('Failed to abort MPU %s on %s/%s', upload_id, bucket, key)


class S3ShardWriter:
    """Manages one MPU: buffers bytes, uploads >=5 MiB parts, completes on close.

    A part that cannot be uploaded (after the retries) makes the writer fail: the MPU is aborted, so no
    object with missing bytes is created, and close() raises instead of completing the upload."""

    def __init__(
        self,
        s3_client,
        shard_key: str,
        dest_bucket: str,
        min_part_size: int,
        max_attempts: int,
        base_backoff_seconds: float,
    ):
        self.s3_client = s3_client
        self.shard_key = shard_key
        self.dest_bucket = dest_bucket
        self.min_part_size = min_part_size
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self.upload_id: Optional[str] = None
        self.part_number = 1
        self.parts: List[Dict] = []
        self.buffer = bytearray()
        self.failed = False

    async def start(self):
        self.upload_id = await mpu_create(
            self.s3_client,
            self.dest_bucket,
            self.shard_key,
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
        )
        logger.info('Started MPU for %s (UploadId=%s)', self.shard_key, self.upload_id)

    async def _upload_part(self, size: int):
        """Upload the first `size` bytes of the buffer as the next part; they leave the buffer only once uploaded."""
        if self.failed:
            raise RuntimeError(f'the upload of {self.shard_key} failed before')
        try:
            etag = await mpu_upload_part(
                self.s3_client,
                self.dest_bucket,
                self.shard_key,
                self.upload_id,
                self.part_number,
                bytes(self.buffer[:size]),
                self.max_attempts,
                self.base_backoff_seconds,
            )
        except BaseException:
            await self.abort()
            raise
        del self.buffer[:size]
        self.parts.append({'PartNumber': self.part_number, 'ETag': etag})
        self.part_number += 1

    async def write(self, data: bytes):
        self.buffer.extend(data)
        while len(self.buffer) >= self.min_part_size:
            await self._upload_part(self.min_part_size)

    async def close(self):
        """Upload the rest and complete the MPU; if that fails, the MPU is aborted."""
        if self.failed:
            raise RuntimeError(f'the upload of {self.shard_key} failed, it was aborted')
        if self.buffer:
            await self._upload_part(len(self.buffer))  # aborts on failure
        try:
            if self.parts:
                await mpu_complete(
                    self.s3_client,
                    self.dest_bucket,
                    self.shard_key,
                    self.upload_id,
                    self.parts,
                    self.max_attempts,
                    self.base_backoff_seconds,
                )
            logger.info('Completed MPU for %s with %d parts.', self.shard_key, len(self.parts))
        except BaseException:
            logger.exception('Completing MPU failed for %s; attempting abort.', self.shard_key)
            await self.abort()
            raise

    async def abort(self):
        """Abort the MPU (no object is created); the writer cannot be used afterwards."""
        if self.failed:
            return
        self.failed = True
        self.buffer.clear()
        if self.upload_id:
            logger.warning('Aborting MPU for %s', self.shard_key)
            await mpu_abort(self.s3_client, self.dest_bucket, self.shard_key, self.upload_id)
