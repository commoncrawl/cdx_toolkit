import pytest

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import patch

from cdx_toolkit.repackage.s3_writer import mpu_abort


from cdx_toolkit.repackage.s3_writer import S3ShardWriter


def test_shard_writer_start():
    """Test ShardWriter start method."""

    async def run_test():
        with patch('cdx_toolkit.repackage.s3_writer.mpu_create') as mock_mpu_create:
            mock_mpu_create.return_value = 'test-upload-id'

            mock_s3 = AsyncMock()

            writer = S3ShardWriter(
                s3_client=mock_s3,
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1024,
                max_attempts=3,
                base_backoff_seconds=0.1,
            )

            await writer.start()

            assert writer.upload_id == 'test-upload-id'
            mock_mpu_create.assert_called_once_with(
                mock_s3,
                'test-bucket',
                'test.warc.gz',
                max_attempts=3,
                base_backoff_seconds=0.1,
            )

    asyncio.run(run_test())


def test_shard_writer_write_small_data():
    """Test ShardWriter write method with small data that stays in buffer."""

    async def run_test():
        mock_s3 = AsyncMock()

        writer = S3ShardWriter(
            s3_client=mock_s3,
            shard_key='test.warc.gz',
            dest_bucket='test-bucket',
            content_type='application/gzip',
            min_part_size=1024,  # 1 KiB
            max_attempts=3,
            base_backoff_seconds=0.1,
        )

        small_data = b'small test data'

        await writer.write(small_data)

        # Data should be in buffer, no parts uploaded yet
        assert len(writer.buffer) == len(small_data)
        assert bytes(writer.buffer) == small_data
        assert writer.part_number == 1
        assert len(writer.parts) == 0

    asyncio.run(run_test())


def test_shard_writer_write_large_data():
    """Test ShardWriter write method with large data that triggers part uploads."""

    async def run_test():
        with patch('cdx_toolkit.repackage.s3_writer.mpu_upload_part') as mock_upload_part:
            mock_upload_part.return_value = 'test-etag-1'

            mock_s3 = AsyncMock()

            writer = S3ShardWriter(
                s3_client=mock_s3,
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=100,  # 100 bytes
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            large_data = b'x' * 250  # 250 bytes, should create 2 parts

            await writer.write(large_data)

            # Should have uploaded 2 parts (100 bytes each) with 50 bytes remaining in buffer
            assert mock_upload_part.call_count == 2
            assert len(writer.parts) == 2
            assert writer.part_number == 3  # Next part would be #3
            assert len(writer.buffer) == 50  # Remaining bytes
            assert bytes(writer.buffer) == b'x' * 50

            # Verify parts structure
            assert writer.parts[0] == {'PartNumber': 1, 'ETag': 'test-etag-1'}
            assert writer.parts[1] == {'PartNumber': 2, 'ETag': 'test-etag-1'}

    asyncio.run(run_test())


def test_shard_writer_close_with_buffer():
    """Test ShardWriter close method with data remaining in buffer."""

    async def run_test():
        with patch('cdx_toolkit.repackage.s3_writer.mpu_upload_part') as mock_upload_part, patch(
            'cdx_toolkit.repackage.s3_writer.mpu_complete'
        ) as mock_complete:
            mock_upload_part.return_value = 'final-etag'

            mock_s3 = AsyncMock()

            writer = S3ShardWriter(
                s3_client=mock_s3,
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1000,  # Large min size to keep data in buffer
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # Add some data to buffer
            remaining_data = b'final chunk data'
            writer.buffer.extend(remaining_data)

            await writer.close()

            # Should upload the final part and complete MPU
            mock_upload_part.assert_called_once_with(
                mock_s3,
                'test-bucket',
                'test.warc.gz',
                'test-upload-id',
                1,  # part number
                remaining_data,
                3,  # max attempts
                0.1,  # base backoff
            )

            mock_complete.assert_called_once_with(
                mock_s3,
                'test-bucket',
                'test.warc.gz',
                'test-upload-id',
                [{'PartNumber': 1, 'ETag': 'final-etag'}],
                3,  # max attempts
                0.1,  # base backoff
            )

            # Buffer should be cleared
            assert len(writer.buffer) == 0
            assert len(writer.parts) == 1

    asyncio.run(run_test())


def test_shard_writer_close_empty():
    """Test ShardWriter close method with no data (empty buffer, no parts)."""

    async def run_test():
        with patch('cdx_toolkit.repackage.s3_writer.mpu_upload_part') as mock_upload_part, patch(
            'cdx_toolkit.repackage.s3_writer.mpu_complete'
        ) as mock_complete:
            mock_s3 = AsyncMock()

            writer = S3ShardWriter(
                s3_client=mock_s3,
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1000,
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # No data in buffer, no parts uploaded
            await writer.close()

            # Should not upload any parts or complete MPU since there's no data
            mock_upload_part.assert_not_called()
            mock_complete.assert_not_called()

            # State should remain unchanged
            assert len(writer.buffer) == 0
            assert len(writer.parts) == 0

    asyncio.run(run_test())


def test_shard_writer_close_with_exception():
    """Test ShardWriter close method with exception and abort handling."""

    async def run_test():
        with patch('cdx_toolkit.repackage.s3_writer.mpu_upload_part') as mock_upload_part, patch(
            'cdx_toolkit.repackage.s3_writer.mpu_complete'
        ) as mock_complete, patch('cdx_toolkit.repackage.s3_writer.mpu_abort') as mock_abort:
            mock_upload_part.return_value = 'error-etag'
            mock_complete.side_effect = Exception('Complete failed')

            mock_s3 = AsyncMock()

            writer = S3ShardWriter(
                s3_client=mock_s3,
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1000,
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # Add some data to buffer to trigger upload and complete
            writer.buffer.extend(b'some data')

            # Should raise the exception after attempting abort
            with pytest.raises(Exception, match='Complete failed'):
                await writer.close()

            # Should have attempted to upload part and complete, then abort on failure
            mock_upload_part.assert_called_once()
            mock_complete.assert_called_once()
            mock_abort.assert_called_once_with(mock_s3, 'test-bucket', 'test.warc.gz', 'test-upload-id')

    asyncio.run(run_test())


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

    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.bodies = {}
        self.completed = None
        self.aborted = False

    async def upload_part(self, PartNumber, Body, **kwargs):
        if self.fail_times:
            self.fail_times -= 1
            from botocore.exceptions import ClientError

            raise ClientError({'Error': {'Code': '500', 'Message': 'boom'}}, 'UploadPart')
        self.bodies[PartNumber] = Body
        return {'ETag': f'etag-{PartNumber}'}

    async def complete_multipart_upload(self, MultipartUpload, **kwargs):
        self.completed = b''.join(self.bodies[p['PartNumber']] for p in MultipartUpload['Parts'])

    async def abort_multipart_upload(self, **kwargs):
        self.aborted = True


def make_writer(s3):
    writer = S3ShardWriter(s3, 'out.warc.gz', 'bucket', None, min_part_size=4, max_attempts=2,
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
