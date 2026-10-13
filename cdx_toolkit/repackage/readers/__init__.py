'''
The readers of WARC ranges, by URL scheme: HTTP(S) (http.py), S3 (aioboto3) and Hugging Face buckets (hf.py).
'''
import asyncio
import contextlib
from typing import Callable

from cdx_toolkit.repackage.readers.hf import make_hf_reader


class Readers:
    """The readers of WARC ranges, by the URL scheme of a range job, each created when it is first needed:
    an S3 client for s3:// and the HF reader for hf:// (http(s):// needs none). They are closed with `stack`."""

    def __init__(self, stack: contextlib.AsyncExitStack, s3_config_factory: Callable):
        self.stack = stack
        self.s3_config_factory = s3_config_factory
        self._s3 = None
        self._hf = None
        self._lock = asyncio.Lock()

    async def s3(self):
        async with self._lock:
            if self._s3 is None:
                import aioboto3

                client = aioboto3.Session().client('s3', config=self.s3_config_factory())
                self._s3 = await self.stack.enter_async_context(client)
            return self._s3

    async def hf(self):
        async with self._lock:
            if self._hf is None:
                self._hf = await self.stack.enter_async_context(make_hf_reader())
            return self._hf
