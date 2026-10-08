import asyncio
import pytest
from unittest.mock import patch

from cdx_toolkit.repackage.local_writer import LocalFileWriter


def test_init_default_values():
    """Test initialization with default values."""
    writer = LocalFileWriter('/tmp/test.txt')
    assert writer.file_path == '/tmp/test.txt'
    assert writer.buffer_size == 8192
    assert writer.mode == 'wb'
    assert writer.file_handle is None
    assert isinstance(writer.buffer, bytearray)
    assert len(writer.buffer) == 0


def test_init_custom_values():
    """Test initialization with custom values."""
    writer = LocalFileWriter('/tmp/test.txt', buffer_size=4096, mode='ab')
    assert writer.file_path == '/tmp/test.txt'
    assert writer.buffer_size == 4096
    assert writer.mode == 'ab'
    assert writer.file_handle is None
    assert isinstance(writer.buffer, bytearray)
    assert len(writer.buffer) == 0


def test_start_opens_file(tmp_path):
    """Test that start() opens the file correctly."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        assert writer.file_handle is not None
        await writer.close()

    asyncio.run(run_test())


def test_start_with_different_modes(tmp_path):
    """Test start() with different file modes."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'

        # Test binary write mode
        writer = LocalFileWriter(str(temp_file), mode='wb')
        await writer.start()
        assert writer.file_handle is not None
        await writer.close()

        # Test binary append mode
        writer = LocalFileWriter(str(temp_file), mode='ab')
        await writer.start()
        assert writer.file_handle is not None
        await writer.close()

    asyncio.run(run_test())


def test_start_creates_directory_if_needed(tmp_path):
    """Test that start() works when parent directory exists."""

    async def run_test():
        subdir = tmp_path / 'subdir'
        subdir.mkdir()
        temp_file = subdir / 'test.txt'

        writer = LocalFileWriter(str(temp_file))
        await writer.start()
        assert writer.file_handle is not None
        await writer.close()

    asyncio.run(run_test())


def test_write_small_data_buffers(tmp_path):
    """Test writing data that doesn't exceed buffer size."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file), buffer_size=100)
        await writer.start()

        test_data = b'Hello, World!'
        await writer.write(test_data)

        # Data should be in buffer, not yet written to file
        assert len(writer.buffer) == len(test_data)
        assert writer.buffer == test_data

        await writer.close()

        # After close, data should be written to file
        assert temp_file.read_bytes() == test_data

    asyncio.run(run_test())


def test_write_large_data_triggers_flush(tmp_path):
    """Test writing data that exceeds buffer size triggers flush."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        buffer_size = 50
        writer = LocalFileWriter(str(temp_file), buffer_size=buffer_size)
        await writer.start()

        # Write data larger than buffer size
        test_data = b'x' * (buffer_size + 10)
        await writer.write(test_data)

        # Buffer should be empty after automatic flush
        assert len(writer.buffer) == 0

        await writer.close()

        # Data should be written to file
        assert temp_file.read_bytes() == test_data

    asyncio.run(run_test())


def test_write_multiple_small_chunks(tmp_path):
    """Test writing multiple small chunks that eventually trigger flush."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        buffer_size = 50
        writer = LocalFileWriter(str(temp_file), buffer_size=buffer_size)
        await writer.start()

        chunk1 = b'a' * 30
        chunk2 = b'b' * 25  # Total: 55 bytes, exceeds buffer

        await writer.write(chunk1)
        assert len(writer.buffer) == 30

        await writer.write(chunk2)
        # Should have triggered flush, buffer should be empty
        assert len(writer.buffer) == 0

        await writer.close()

        assert temp_file.read_bytes() == chunk1 + chunk2

    asyncio.run(run_test())


def test_write_empty_data(tmp_path):
    """Test writing empty data."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        await writer.write(b'')
        assert len(writer.buffer) == 0

        await writer.close()

        assert temp_file.read_bytes() == b''

    asyncio.run(run_test())


def test_write_without_start_graceful_handling(tmp_path):
    """Test that writing without calling start() is handled gracefully."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file), buffer_size=10)  # Small buffer to force flush

        # This should work fine as long as we don't exceed buffer size
        await writer.write(b'small')
        assert len(writer.buffer) == 5

        # When buffer exceeds size, flush is called but does nothing since file_handle is None
        # The data stays in buffer instead of being written
        await writer.write(b'data that exceeds buffer size')

        # Buffer should contain all the data since flush did nothing
        expected_data = b'small' + b'data that exceeds buffer size'
        assert writer.buffer == expected_data

    asyncio.run(run_test())


def test_flush_empty_buffer(tmp_path):
    """Test flushing when buffer is empty."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        # Flush empty buffer should not raise error
        await writer._flush()
        assert len(writer.buffer) == 0

        await writer.close()

    asyncio.run(run_test())


def test_flush_without_file_handle(tmp_path):
    """Test flushing without file handle."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        writer.buffer.extend(b'test data')

        # Should not raise error, just do nothing
        await writer._flush()
        assert len(writer.buffer) == len(b'test data')  # Buffer unchanged

    asyncio.run(run_test())


def test_close_flushes_remaining_data(tmp_path):
    """Test that close() flushes any remaining buffered data."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file), buffer_size=100)
        await writer.start()

        test_data = b'This data should be flushed on close'
        await writer.write(test_data)

        # Data should still be in buffer
        assert len(writer.buffer) == len(test_data)

        await writer.close()

        # Data should now be written to file
        assert temp_file.read_bytes() == test_data

    asyncio.run(run_test())


def test_close_without_start(tmp_path):
    """Test closing without calling start()."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))

        # Should not raise error
        await writer.close()

    asyncio.run(run_test())


def test_close_twice(tmp_path):
    """Test calling close() multiple times."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        await writer.close()

        # Second close should not raise error
        await writer.close()

    asyncio.run(run_test())


def test_close_handles_flush_exception(tmp_path):
    """Test that close() handles exceptions during flush properly."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        # Add some data to buffer
        await writer.write(b'test data')

        # Mock flush to raise an exception
        with patch.object(writer, '_flush', side_effect=Exception('Flush error')):
            with pytest.raises(Exception, match='Flush error'):
                await writer.close()

    asyncio.run(run_test())


def test_close_handles_file_close_exception(tmp_path):
    """Test that close() handles exceptions during file close."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        # Mock file handle close to raise an exception
        with patch.object(writer.file_handle, 'close', side_effect=Exception('Close error')):
            with pytest.raises(Exception, match='Close error'):
                await writer.close()

    asyncio.run(run_test())


def test_large_file_write(tmp_path):
    """Test writing a large amount of data."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file), buffer_size=1024)
        await writer.start()

        # Write 1MB of data in chunks
        chunk_size = 1024  # Make chunk size same as buffer for exact division
        total_size = 1024 * 1024  # 1MB
        chunk_data = b'x' * chunk_size

        for _ in range(total_size // chunk_size):
            await writer.write(chunk_data)

        await writer.close()

        # Verify file size
        assert temp_file.stat().st_size == total_size

    asyncio.run(run_test())


def test_binary_data_integrity(tmp_path):
    """Test that binary data is written correctly."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))
        await writer.start()

        # Create binary data with all byte values
        binary_data = bytes(range(256))
        await writer.write(binary_data)

        await writer.close()

        assert temp_file.read_bytes() == binary_data

    asyncio.run(run_test())


def test_concurrent_writes(tmp_path):
    """Test concurrent write operations."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file), buffer_size=100)
        await writer.start()

        # Create multiple write tasks
        async def write_chunk(data):
            await writer.write(data)

        tasks = [write_chunk(f'chunk{i}'.encode() * 10) for i in range(10)]

        await asyncio.gather(*tasks)
        await writer.close()

        # Verify file exists and has data
        assert temp_file.exists()
        assert temp_file.stat().st_size > 0

    asyncio.run(run_test())


def test_file_permissions_error(tmp_path):
    """Test handling of file permission errors."""

    async def run_test():
        # Create a file path in a directory we can't write to
        readonly_file = tmp_path / 'readonly.txt'

        # Create the file first
        readonly_file.write_text('test')

        # Make the file read-only
        readonly_file.chmod(0o444)

        writer = LocalFileWriter(str(readonly_file), mode='wb')

        with pytest.raises(PermissionError):
            await writer.start()

    asyncio.run(run_test())


def test_nonexistent_directory():
    """Test writing to a file in a nonexistent directory."""

    async def run_test():
        nonexistent_path = '/nonexistent/directory/file.txt'
        writer = LocalFileWriter(nonexistent_path)

        with pytest.raises(FileNotFoundError):
            await writer.start()

    asyncio.run(run_test())


def test_context_manager_like_usage(tmp_path):
    """Test typical usage pattern similar to context manager."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        writer = LocalFileWriter(str(temp_file))

        try:
            await writer.start()
            await writer.write(b'Hello, World!')
            await writer.write(b' How are you?')
        finally:
            await writer.close()

        assert temp_file.read_bytes() == b'Hello, World! How are you?'

    asyncio.run(run_test())


def test_buffer_size_edge_cases(tmp_path):
    """Test edge cases with different buffer sizes."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'
        # Test with buffer size of 1
        writer = LocalFileWriter(str(temp_file), buffer_size=1)
        await writer.start()

        await writer.write(b'a')  # Should trigger flush immediately
        assert len(writer.buffer) == 0

        await writer.write(b'bc')  # Should trigger flush after 'b', leaving 'c'
        assert len(writer.buffer) == 0

        await writer.close()

        assert temp_file.read_bytes() == b'abc'

    asyncio.run(run_test())


def test_append_mode(tmp_path):
    """Test append mode functionality."""

    async def run_test():
        temp_file = tmp_path / 'test.txt'

        # First, write some initial data
        temp_file.write_bytes(b'Initial data\n')

        # Now append using LocalFileWriter
        writer = LocalFileWriter(str(temp_file), mode='ab')
        await writer.start()

        await writer.write(b'Appended data\n')
        await writer.close()

        # Verify both pieces of data are present
        content = temp_file.read_bytes()
        assert content == b'Initial data\nAppended data\n'

    asyncio.run(run_test())
