'''
The output files of cdxt repackage: a local file (local.py) or an S3 object written with a multipart upload
(s3.py), and the series of WARC files they make up (series.py).
'''
from abc import ABC, abstractmethod

from cdx_toolkit.repackage.urls import is_s3_url, parse_s3_uri


class OutputFile(ABC):
    """One output file, written in one go: start(), write()..., then close().

    close() completes the file; if that fails, the file aborts itself (and close() raises). abort() removes the
    incomplete file (or aborts its upload), also after a failed start(); it can be called more than once."""

    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def write(self, data: bytes) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def abort(self) -> None:
        raise NotImplementedError  # pragma: no cover


def open_output_file(path: str, s3_client=None, min_part_size: int = 5 * 1024 * 1024, max_attempts: int = 5,
                     base_backoff_seconds: float = 0.5) -> OutputFile:
    """A new (not started) output file: an S3 object for s3://bucket/key (written with `s3_client`), else a local
    file."""
    if is_s3_url(path):
        from cdx_toolkit.repackage.writers.s3 import S3OutputFile

        bucket, key = parse_s3_uri(path)
        return S3OutputFile(s3_client, bucket, key, min_part_size, max_attempts, base_backoff_seconds)

    from cdx_toolkit.repackage.writers.local import LocalOutputFile

    return LocalOutputFile(path)
