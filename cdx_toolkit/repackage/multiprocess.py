"""Multi-process repackage: shard the range jobs and fetch each shard in its own process
(one asyncio event loop per CPU core), all writing into one series of output WARCs.

A single asyncio loop saturates one CPU core on many small range-GETs, so reaching
all cores needs multiple processes. This orchestrator keeps that entirely inside
``cdxt repackage`` (no external driver):

1. Drain the configured source once and split it into N self-contained shard CSVs,
   hashed by WARC filename so every record of a file stays in one shard.
2. Run one worker process per (non-empty) shard concurrently. Each worker **rotates its
   output at ``--size``**; every output file is a self-contained WARC with its own
   ``warcinfo`` record.
3. The file numbers come from **one counter shared by all workers**: whenever a worker
   starts a new file it takes the next number, so the files are written under their final
   names ``<prefix>[-<subprefix>]-001.warc.gz``, ``-002`` … right away (and the ``warcinfo``
   record names the file it is in). Which worker writes which number depends on timing.
   Output honours ``--size`` as single-process mode does (files are a target size; the last
   file of each worker may be short, so up to N short files appear).

The WARC writer supports local and S3 output. For other fsspec destinations (e.g. an
``hf://`` bucket) the files are staged in a local directory under their final names and
copied to the destination afterwards.
"""
import glob
import hashlib
import logging
import multiprocessing
import os
import posixpath
import shutil
import tempfile
import time

import fsspec
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

from cdx_toolkit.aws import configure_fsspec_s3
from cdx_toolkit.repackage.data_classes import RepackageResult
from cdx_toolkit.repackage.sources.csv import CsvSource, RangeJobCsvWriter
from cdx_toolkit.repackage.warc_filter import WARCFilter
from cdx_toolkit.repackage.warc_utils import generate_warc_filename
from cdx_toolkit.repackage.s3_utils import is_s3_url, parse_s3_uri

logger = logging.getLogger(__name__)

# Concurrency for copying staged files to a non-S3 remote destination (override with CDXT_COPY_CONCURRENCY).
_COPY_CONCURRENCY = int(os.environ.get('CDXT_COPY_CONCURRENCY', '16'))

# The shared counter of output file numbers, set in each worker process by _init_worker.
_FILE_SEQUENCE = None


def _output_filename(prefix_path: str, sequence: int, subprefix: Optional[str] = None,
                     gzip: bool = True) -> str:
    """Build a WARC output path for a given rotation sequence (S3 uri or local path)."""
    if is_s3_url(prefix_path):
        bucket, key_prefix = parse_s3_uri(prefix_path)
        key = generate_warc_filename(key_prefix, sequence=sequence, writer_subprefix=subprefix, gzip=gzip)
        return f's3://{bucket}/{key}'
    return generate_warc_filename(prefix_path, sequence=sequence, writer_subprefix=subprefix, gzip=gzip)


def _init_worker(file_sequence) -> None:
    global _FILE_SEQUENCE
    _FILE_SEQUENCE = file_sequence
    configure_fsspec_s3()  # a new process does not have the fsspec configuration of the main process


def _next_file_sequence(_current: int) -> int:
    """Take the next output file number from the counter shared by all workers."""
    with _FILE_SEQUENCE.get_lock():
        _FILE_SEQUENCE.value += 1
        return _FILE_SEQUENCE.value


def _run_worker(cfg: Dict) -> RepackageResult:
    """Worker entrypoint (runs in a child process): fetch one shard into the output WARCs."""
    if cfg.get('uvloop'):
        os.environ['CDXT_UVLOOP'] = '1'
    source = CsvSource(cfg['shard_csv'], warc_download_prefix=None, sort=True)
    wf = WARCFilter(
        source=source,
        prefix_path=cfg['prefix_path'],
        writer_info=cfg['writer_info'],
        writer_subprefix=cfg['subprefix'],
        write_paths_as_metadata_records=cfg['write_paths_as_metadata_records'],
        log_every_n=cfg['log_every_n'],
        n_parallel_readers=cfg['readers'],
        aws_region_name=cfg['aws_region_name'],
        max_attempts=cfg['max_attempts'],
        max_file_size=cfg['max_file_size'],  # rotate at --size
        next_file_sequence=_next_file_sequence,
    )
    return wf.filter()


def _split_source_to_shards(source, n: int, out_dir: str) -> Tuple[List[str], List[int]]:
    """Drain the source once into N self-contained shard CSVs, hashed by WARC file.

    Returns the shard paths and the number of range jobs in each shard."""
    paths = [os.path.join(out_dir, f'shard.{i}.csv') for i in range(n)]
    writers = [RangeJobCsvWriter(p) for p in paths]
    counts = [0] * n
    total = 0
    try:
        for job in source.iter_range_jobs():
            key = (job.filename or job.url or '').encode()
            idx = int(hashlib.md5(key).hexdigest(), 16) % n
            writers[idx].write(job)
            counts[idx] += 1
            total += 1
    finally:
        for w in writers:
            w.close()
    logger.info('Sharded %d range jobs into %d shards: %s', total, n, counts)
    return paths, counts


def run_multiprocess_repackage(
    *,
    source,
    n_processes: int,
    readers_per_process: int,
    prefix_path: str,
    writer_info: Dict,
    writer_subprefix: Optional[str],
    write_paths_as_metadata_records: Optional[List[str]],
    log_every_n: int,
    aws_region_name: Optional[str] = None,
    max_attempts: int,
    uvloop: bool,
    max_file_size: Optional[int] = 1_000_000_000,
) -> RepackageResult:
    """Shard -> N worker processes (rotating at --size) writing one global
    <prefix>[-<subprefix>]-NNN.warc.gz series."""
    tmp_dir = tempfile.mkdtemp(prefix='cdxt_shards_')
    start = time.time()
    try:
        shard_csvs, counts = _split_source_to_shards(source, n_processes, tmp_dir)
        # empty shards would only produce WARC files without records
        shard_csvs = [path for path, count in zip(shard_csvs, counts) if count > 0] or shard_csvs[:1]

        # The WARC writer supports S3 and local FS only. Other fsspec destinations
        # (e.g. an hf:// bucket) are staged in a local dir and copied afterwards.
        remote_non_s3 = ('://' in prefix_path) and not is_s3_url(prefix_path)
        staging_dir = os.path.join(tmp_dir, 'out')
        if remote_non_s3:
            os.makedirs(staging_dir)
        # staged under the final file names, so that the warcinfo records name the files they are in
        write_prefix = os.path.join(staging_dir, posixpath.basename(prefix_path)) if remote_non_s3 else prefix_path

        configs = [
            dict(
                shard_csv=shard_csv,
                prefix_path=write_prefix,
                subprefix=writer_subprefix,
                writer_info=writer_info,
                write_paths_as_metadata_records=write_paths_as_metadata_records,
                log_every_n=log_every_n,
                readers=readers_per_process,
                aws_region_name=aws_region_name,
                max_attempts=max_attempts,
                max_file_size=max_file_size,
                uvloop=uvloop,
            )
            for shard_csv in shard_csvs
        ]

        logger.info('Launching %d worker processes x %d readers', len(configs), readers_per_process)
        mp_context = multiprocessing.get_context()
        file_sequence = mp_context.Value('i', 0)
        with ProcessPoolExecutor(
            max_workers=len(configs), mp_context=mp_context, initializer=_init_worker, initargs=(file_sequence,)
        ) as ex:
            results = list(ex.map(_run_worker, configs))
        result = sum(results, RepackageResult())
        total = result.written
        n_files = file_sequence.value
        fetch_elapsed = time.time() - start
        logger.info('All %d workers done: %d records in %d files in %.1fs (%.0f rec/s)',
                    len(configs), total, n_files, fetch_elapsed, total / fetch_elapsed if fetch_elapsed else 0)

        if remote_non_s3:
            _copy_staged_files(write_prefix, prefix_path)

        logger.info('Repackage complete -> %d files %s (%.1fs total)',
                    n_files, _output_filename(prefix_path, sequence=1, subprefix=writer_subprefix),
                    time.time() - start)
        return result
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _copy_staged_files(staging_prefix: str, prefix_path: str) -> List[str]:
    """Copy the locally staged <staging_prefix>-... files to <prefix_path>-... (e.g. on hf://)."""
    staged = sorted(glob.glob(f'{staging_prefix}-*.warc.gz'))

    def _copy(src):
        dest = prefix_path + src[len(staging_prefix):]
        with open(src, 'rb') as fi, fsspec.open(dest, 'wb') as fo:
            shutil.copyfileobj(fi, fo, length=8 * 1024 * 1024)
        logger.info('Wrote %s', dest)
        return dest

    with ThreadPoolExecutor(max_workers=max(1, min(_COPY_CONCURRENCY, len(staged)))) as ex:
        return list(ex.map(_copy, staged))
