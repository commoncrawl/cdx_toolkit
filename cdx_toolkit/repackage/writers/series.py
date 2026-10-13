'''
The output of a run: the series of WARC files <prefix>[-<subprefix>]-NNN.warc.gz.
'''
import glob
import logging
import os
import posixpath
import re
from typing import Callable, Dict, List, Optional

import fsspec

from cdx_toolkit.repackage.urls import is_s3_url
from cdx_toolkit.repackage.warc_records import (
    get_bytes_from_warc_record, get_metadata_record_from_path, warcinfo_record_bytes,
)
from cdx_toolkit.repackage.writers import OutputFile, open_output_file


logger = logging.getLogger(__name__)


def generate_warc_filename(
    dest_prefix: str,
    sequence: int,
    writer_subprefix: Optional[str] = None,
    gzip: bool = False,
) -> str:
    """Generate a WARC file name from a prefix (can be a full path), an optional subprefix
    (used to distinguish per-process shards), and a sequence index (file-rotation counter)."""
    file_name = dest_prefix + '-'
    if writer_subprefix is not None:
        file_name += writer_subprefix + '-'
    file_name += f'{sequence:03d}.warc'  # TODO default warc command uses ".extracted.warc"
    if gzip:
        file_name += '.gz'

    return file_name


def existing_warc_files(prefix_path: str, writer_subprefix: Optional[str] = None, gzip: bool = True) -> List[str]:
    """The files of the series <prefix>[-<subprefix>]-NNN.warc[.gz] (async WARC fetcher) or
    <prefix>[-<subprefix>]-NNNNNN.extracted.warc[.gz] (--wb) that already exist (local, s3://, hf://, ...)."""
    fs, fs_prefix = fsspec.url_to_fs(prefix_path)
    name = re.escape(posixpath.basename(fs_prefix)) + '-'
    if writer_subprefix is not None:
        name += re.escape(writer_subprefix) + '-'
    name += r'\d{3,}(\.extracted)?\.warc' + (r'\.gz' if gzip else '')
    return sorted(
        fs.unstrip_protocol(path) for path in fs.glob(glob.escape(fs_prefix) + '-*')
        if re.fullmatch(name, posixpath.basename(path))
    )


class WarcFileSeries:
    """The WARC files <prefix>[-<subprefix>]-NNN.warc.gz of a run, written one after the other.

    The first file is started with the first record, so no file is written without records. Each file starts with
    its warcinfo record (and the metadata records, if any); a new file is started when the next record would make
    the current one larger than `max_file_size` (each file gets at least one record).

    A failure is fatal: the file it happened in is aborted (removed, or its upload aborted), the files completed
    before are kept."""

    def __init__(
        self,
        prefix_path: str,
        writer_info: Dict,
        writer_subprefix: Optional[str] = None,
        metadata_paths: Optional[List[str]] = None,
        warc_version: str = '1.0',
        gzip: bool = True,
        max_file_size: Optional[int] = None,
        next_file_sequence: Optional[Callable[[int], int]] = None,
        s3_client=None,
        min_part_size: int = 5 * 1024 * 1024,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
    ):
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.metadata_paths = metadata_paths or []
        self.warc_version = warc_version
        self.gzip = gzip
        self.max_file_size = max_file_size
        # Number of the next file, given the current one (0 before the first file). The multi-process mode
        # shares one counter across processes, so file names are final right away.
        self.next_file_sequence = next_file_sequence or (lambda sequence: sequence + 1)
        self.s3_client = s3_client
        self.min_part_size = min_part_size
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds

        self.sequence = 0  # of the current (or last) file
        self._file: Optional[OutputFile] = None
        self._file_size = 0
        self._records_in_file = 0

    async def write(self, record: bytes) -> None:
        """Write a (complete, possibly gzipped) WARC record, in a new file if needed."""
        if self._file is None:
            await self._start_file(self.next_file_sequence(0))
        elif self._records_in_file and self.max_file_size and self._file_size + len(record) > self.max_file_size:
            await self._close_file()  # the finished file is kept if starting the next one fails
            await self._start_file(self.next_file_sequence(self.sequence))
            logger.info(f'Rotated to new WARC file sequence {self.sequence} due to size limit')
        await self._file.write(record)
        self._records_in_file += 1
        self._file_size += len(record)

    async def close(self) -> None:
        """Complete the current file (if it cannot be completed, it is aborted)."""
        if self._file is not None:
            await self._close_file()

    async def abort(self) -> None:
        """Abort the current file (no-op if there is none, e.g. after a failure that aborted it already)."""
        file, self._file = self._file, None
        if file is not None:
            await file.abort()

    async def _close_file(self) -> None:
        file, self._file = self._file, None
        await file.close()  # aborts itself if it fails

    async def _start_file(self, sequence: int) -> None:
        """Start the file with its warcinfo record (and the metadata records); if that fails, it is aborted."""
        path = generate_warc_filename(self.prefix_path, sequence, self.writer_subprefix, self.gzip)
        file = open_output_file(path, self.s3_client, self.min_part_size, self.max_attempts, self.base_backoff_seconds)
        # the name in the warcinfo record (local paths may have backslashes on Windows)
        name = posixpath.basename(path) if is_s3_url(path) else os.path.basename(path)
        logger.debug('Starting WARC file %s', name)
        try:
            await file.start()
            header, warcinfo_id = warcinfo_record_bytes(name, self.writer_info, self.warc_version, self.gzip)
            await file.write(header)
            size = len(header)
            if self.metadata_paths:
                size += await self._write_metadata_records(file, warcinfo_id)
        except BaseException:
            await file.abort()
            raise
        self._file, self.sequence, self._file_size, self._records_in_file = file, sequence, size, 0

    async def _write_metadata_records(self, file: OutputFile, warcinfo_id: str) -> int:
        """Write a metadata record for each of the metadata paths; returns their size."""
        size = 0
        logger.info(f'Writing {len(self.metadata_paths)} metadata records to WARC ... ')
        for path in self.metadata_paths:
            logger.info(f'Writing metadata record from {path} ...')
            record = get_metadata_record_from_path(file_path=path, warcinfo_id=warcinfo_id)
            data = get_bytes_from_warc_record(record, warc_version=self.warc_version, gzip=self.gzip)
            await file.write(data)
            size += len(data)
        logger.info(f'Metadata records added: {len(self.metadata_paths)}')
        return size
