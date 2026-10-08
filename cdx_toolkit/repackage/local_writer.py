import aiofiles


class LocalFileWriter:
    """Async writer for local file system using aiofiles."""

    def __init__(self, file_path: str, buffer_size: int = 8192, mode: str = 'wb'):
        self.file_path = file_path
        self.buffer_size = buffer_size
        self.mode = mode
        self.file_handle = None
        self.buffer = bytearray()

    async def start(self):
        self.file_handle = await aiofiles.open(self.file_path, self.mode)

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
        try:
            if self.buffer:
                await self._flush()
            if self.file_handle:
                await self.file_handle.close()
        except Exception:
            if self.file_handle:
                await self.file_handle.close()
            raise
