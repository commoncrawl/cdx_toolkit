import asyncio
import os

import pytest

from cdx_toolkit.repackage.local_writer import LocalFileWriter


def run(coro):
    return asyncio.run(coro)


def test_writes_buffered_and_byte_exact(tmp_path):
    path = tmp_path / 'out.warc.gz'
    chunks = [bytes(range(256)), b'small', b'x' * 20000, b'', b'\x00\xff' * 10]

    async def write():
        writer = LocalFileWriter(str(path), buffer_size=1024)
        await writer.start()
        await writer.write(chunks[1])
        assert path.read_bytes() == b''  # buffered
        await writer.write(chunks[2])
        assert len(path.read_bytes()) == len(chunks[1] + chunks[2])  # flushed at the buffer size
        for chunk in chunks[3:]:
            await writer.write(chunk)
        await writer.close()

    run(write())
    assert path.read_bytes() == b''.join(chunks[1:])


def test_close_without_start(tmp_path):
    run(LocalFileWriter(str(tmp_path / 'out')).close())
    assert not (tmp_path / 'out').exists()


def test_abort_removes_the_file(tmp_path):
    path = tmp_path / 'out.warc.gz'

    async def write_and_abort():
        writer = LocalFileWriter(str(path), buffer_size=4)
        await writer.start()
        await writer.write(b'incomplete')
        await writer.abort()
        await writer.abort()  # idempotent

    run(write_and_abort())
    assert not path.exists()
    run(LocalFileWriter(str(path)).abort())  # without start


def test_start_fails_without_the_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        run(LocalFileWriter(os.path.join(str(tmp_path), 'missing', 'out')).start())


def test_close_closes_the_file_if_the_last_write_fails(tmp_path):
    async def close_with_failing_flush():
        writer = LocalFileWriter(str(tmp_path / 'out'))
        await writer.start()
        await writer.write(b'x')
        handle = writer.file_handle

        async def fail():
            raise OSError('disk full')

        writer._flush = fail
        with pytest.raises(OSError):
            await writer.close()
        assert writer.file_handle is None and handle.closed

    run(close_with_failing_flush())
