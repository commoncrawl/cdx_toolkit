import asyncio
from unittest.mock import AsyncMock

import pytest

from cdx_toolkit.repackage.s3_writer import S3ShardWriter, mpu_abort


def test_mpu_abort_success():
    """Test mpu_abort function with successful abort."""

    async def run_test():
        mock_s3 = AsyncMock()
        bucket = 'test-bucket'
        key = 'test-key'
        upload_id = 'test-upload-id'

        await mpu_abort(mock_s3, bucket, key, upload_id)

        mock_s3.abort_multipart_upload.assert_called_once_with(Bucket=bucket, Key=key, UploadId=upload_id)

    asyncio.run(run_test())


def test_mpu_abort_with_exception():
    """Test mpu_abort function when abort fails (should catch exception)."""

    async def run_test():
        mock_s3 = AsyncMock()
        mock_s3.abort_multipart_upload.side_effect = Exception('S3 error')

        bucket = 'test-bucket'
        key = 'test-key'
        upload_id = 'test-upload-id'

        # Should not raise exception, should log it instead
        await mpu_abort(mock_s3, bucket, key, upload_id)

        mock_s3.abort_multipart_upload.assert_called_once_with(Bucket=bucket, Key=key, UploadId=upload_id)

    asyncio.run(run_test())


class FakeS3:
    """upload_part fails `fail_times` times (ClientError), then stores the part bodies."""

    def __init__(self, fail_times=0, fail_complete=False):
        self.fail_times = fail_times
        self.fail_complete = fail_complete
        self.bodies = {}
        self.completed = None
        self.aborted = False

    async def create_multipart_upload(self, Bucket, Key):
        return {'UploadId': 'upload-id'}

    async def upload_part(self, PartNumber, Body, **kwargs):
        if self.fail_times:
            self.fail_times -= 1
            from botocore.exceptions import ClientError

            raise ClientError({'Error': {'Code': '500', 'Message': 'boom'}}, 'UploadPart')
        self.bodies[PartNumber] = Body
        return {'ETag': f'etag-{PartNumber}'}

    async def complete_multipart_upload(self, MultipartUpload, **kwargs):
        if self.fail_complete:
            raise RuntimeError('complete failed')
        self.completed = b''.join(self.bodies[p['PartNumber']] for p in MultipartUpload['Parts'])

    async def abort_multipart_upload(self, **kwargs):
        self.aborted = True


def make_writer(s3):
    writer = S3ShardWriter(s3, 'out.warc.gz', 'bucket', min_part_size=4, max_attempts=2,
                           base_backoff_seconds=0)
    writer.upload_id = 'upload-id'
    return writer


def test_shard_writer_failed_part_aborts_the_upload():
    pytest.importorskip('botocore')
    s3 = FakeS3(fail_times=2)  # more than max_attempts
    writer = make_writer(s3)

    async def run():
        with pytest.raises(Exception, match='boom'):
            await writer.write(b'ABCD')
        with pytest.raises(RuntimeError):
            await writer.write(b'EFGH')
        with pytest.raises(RuntimeError, match='aborted'):
            await writer.close()

    asyncio.run(run())
    assert s3.aborted
    assert s3.completed is None


def test_shard_writer_retried_part_keeps_every_byte_once():
    pytest.importorskip('botocore')
    s3 = FakeS3(fail_times=1)  # less than max_attempts
    writer = make_writer(s3)

    async def run():
        await writer.write(b'ABCDEF')
        await writer.write(b'GHIJ')
        await writer.close()

    asyncio.run(run())
    assert s3.completed == b'ABCDEFGHIJ'
    assert not s3.aborted


def test_shard_writer_uploads_parts_of_the_minimum_size():
    s3 = FakeS3()
    writer = make_writer(s3)

    async def run():
        await writer.start()
        await writer.write(b'AB')
        assert s3.bodies == {}  # buffered
        await writer.write(b'CDEFGHIJK')
        assert s3.bodies == {1: b'ABCD', 2: b'EFGH'}
        await writer.close()

    asyncio.run(run())
    assert s3.bodies[3] == b'IJK'
    assert s3.completed == b'ABCDEFGHIJK'


def test_shard_writer_without_data_completes_nothing():
    s3 = FakeS3()
    asyncio.run(make_writer(s3).close())
    assert s3.completed is None and not s3.aborted


def test_shard_writer_failed_complete_aborts_the_upload():
    s3 = FakeS3(fail_complete=True)
    writer = make_writer(s3)

    async def run():
        await writer.write(b'AB')
        with pytest.raises(RuntimeError, match='complete failed'):
            await writer.close()

    asyncio.run(run())
    assert s3.aborted
