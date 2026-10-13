import asyncio
import os

import aiofiles

from cdx_toolkit.repackage.writers import OutputFile


class LocalOutputFile(OutputFile):
    """A local file, written with aiofiles; abort() removes it."""

    def __init__(self, file_path: str, buffer_size: int = 8192, mode: str = 'wb'):
        self.file_path = file_path
        self.buffer_size = buffer_size
        self.mode = mode
        self.file_handle = None
        self.buffer = bytearray()

    async def start(self):
        opening = asyncio.ensure_future(aiofiles.open(self.file_path, self.mode))
        try:
            self.file_handle = await asyncio.shield(opening)
        except asyncio.CancelledError:
            # the file is created anyway: finish opening it, so that abort() can remove it
            self.file_handle = await opening
            raise

    async def write(self, data: bytes):
        self.buffer.extend(data)
        if len(self.buffer) >= self.buffer_size:
            await self._flush()

    async def _flush(self):
        if self.buffer and self.file_handle:
            await self.file_handle.write(bytes(self.buffer))
            await self.file_handle.flush()
            self.buffer.clear()

    async def close(self):
        """Flush and close the file; if that fails, the (incomplete) file is removed."""
        try:
            if self.buffer:
                await self._flush()
            if self.file_handle:
                file_handle, self.file_handle = self.file_handle, None
                await file_handle.close()
        except BaseException:
            await self.abort()
            raise

    async def abort(self):
        """Close and remove the (incomplete) file."""
        self.buffer.clear()
        if self.file_handle:
            await self.file_handle.close()
            self.file_handle = None
        try:
            os.remove(self.file_path)
        except FileNotFoundError:
            pass
