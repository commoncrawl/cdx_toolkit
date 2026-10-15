"""Unit tests for multi-process output naming: the file numbers shared by all worker
processes, and copying staged files to their (non-S3) destination.

No S3 or fetching.
"""
import multiprocessing
import os

from cdx_toolkit.repackage import multiprocess
from cdx_toolkit.repackage.multiprocess import _copy_staged_files
from cdx_toolkit.repackage.writers.series import generate_warc_filename


def test_warc_filenames():
    # the formatter of the file names, also for the log line of the multi-process mode
    assert generate_warc_filename('/out/run', 1, gzip=True) == '/out/run-001.warc.gz'
    assert generate_warc_filename('/out/run', 12, gzip=True) == '/out/run-012.warc.gz'
    assert generate_warc_filename('/out/run', 1234, gzip=True) == '/out/run-1234.warc.gz'
    assert generate_warc_filename('/out/run', 2, writer_subprefix='SUB', gzip=True) == '/out/run-SUB-002.warc.gz'
    assert generate_warc_filename('s3://b/p/run', 3, gzip=True) == 's3://b/p/run-003.warc.gz'
    assert generate_warc_filename('hf://buckets/a/b/run', 3) == 'hf://buckets/a/b/run-003.warc'


def _take_numbers(n):
    return [multiprocess._next_file_sequence(0) for _ in range(n)]


def test_file_numbers_are_shared_across_processes():
    ctx = multiprocessing.get_context()
    counter = ctx.Value('i', 0)
    results = multiprocess.run_worker_processes(_take_numbers, [5, 5, 5], counter, ctx)
    numbers = [n for result in results for n in result]

    # every file number is used exactly once, without gaps
    assert sorted(numbers) == list(range(1, 16))
    assert counter.value == 15


def test_copy_staged_files(tmp_path):
    staging = tmp_path / 'staging'
    staging.mkdir()
    for name in ('warc-001.warc.gz', 'warc-002.warc.gz', 'other.txt'):
        (staging / name).write_bytes(name.encode())
    dest_dir = tmp_path / 'dest'
    dest_dir.mkdir()

    copied = _copy_staged_files(str(staging / 'warc'), str(dest_dir / 'RUN'))

    assert copied == [str(dest_dir / 'RUN-001.warc.gz'), str(dest_dir / 'RUN-002.warc.gz')]
    assert sorted(os.listdir(dest_dir)) == ['RUN-001.warc.gz', 'RUN-002.warc.gz']
    assert (dest_dir / 'RUN-002.warc.gz').read_bytes() == b'warc-002.warc.gz'
