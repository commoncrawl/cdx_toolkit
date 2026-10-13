'''
The readers of jobs: WARC ranges, one reader per storage (HTTP(S) in http.py, S3 in s3.py, Hugging Face buckets
in hf.py), and captures replayed from a wayback machine (wayback.py).
'''
from abc import ABC, abstractmethod
import asyncio
import contextlib
from typing import Callable

from cdx_toolkit.repackage.jobs import Job, WaybackJob
from cdx_toolkit.repackage.urls import is_hf_url, is_s3_url


class RangeReader(ABC):
    """Reads the bytes of a job: a range from its storage, or a wayback replay."""

    @abstractmethod
    async def read(self, job: Job) -> bytes:
        """For a RangeJob exactly the bytes job.offset .. job.offset + job.length - 1 of job.url; raises (after the
        reader's retries) if they cannot be read, e.g. RangeError for other bytes than requested."""
        raise NotImplementedError  # pragma: no cover


class Readers:
    """The reader of each job: the wayback reader for a WaybackJob; for a RangeJob by the URL scheme: S3 for
    s3://, the HF reader for hf://, HTTP otherwise. The S3 and HF readers are created when they are first needed,
    and closed with `stack`."""

    def __init__(self, stack: contextlib.AsyncExitStack, s3_config_factory: Callable, max_attempts: int = 5,
                 base_backoff_seconds: float = 0.5):
        from cdx_toolkit.repackage.readers.http import HttpRangeReader
        from cdx_toolkit.repackage.readers.wayback import WaybackReader

        self.stack = stack
        self.s3_config_factory = s3_config_factory
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self._http = HttpRangeReader()
        self._wayback = WaybackReader()
        self._s3 = None
        self._hf = None
        self._lock = asyncio.Lock()

    async def for_job(self, job: Job) -> RangeReader:
        if isinstance(job, WaybackJob):
            return self._wayback
        url = job.url
        if is_s3_url(url):
            return await self._s3_reader()
        if is_hf_url(url):
            return await self._hf_reader()
        return self._http

    async def _s3_reader(self) -> RangeReader:
        async with self._lock:
            if self._s3 is None:
                import aioboto3

                from cdx_toolkit.repackage.readers.s3 import S3RangeReader

                client = aioboto3.Session().client('s3', config=self.s3_config_factory())
                client = await self.stack.enter_async_context(client)
                self._s3 = S3RangeReader(client, self.max_attempts, self.base_backoff_seconds)
            return self._s3

    async def _hf_reader(self) -> RangeReader:
        async with self._lock:
            if self._hf is None:
                from cdx_toolkit.repackage.readers import hf

                reader = hf.make_hf_reader(self.max_attempts, self.base_backoff_seconds)
                self._hf = await self.stack.enter_async_context(reader)
            return self._hf
