"""The series of output WARC files: numbering, rotation, warcinfo records, and aborting on failure."""
import asyncio
import glob
import os
from io import BytesIO
from unittest.mock import patch

import pytest
from warcio.archiveiterator import ArchiveIterator
from warcio.recordbuilder import RecordBuilder

from cdx_toolkit.repackage.warc_records import get_bytes_from_warc_record
from cdx_toolkit.repackage.writers.local import LocalOutputFile
from cdx_toolkit.repackage.writers.series import WarcFileSeries


def record(url: str) -> bytes:
    rec = RecordBuilder().create_warc_record(url, 'resource', payload=BytesIO(b'x' * 100))
    return get_bytes_from_warc_record(rec, gzip=True)


def records_by_file(prefix):
    result = {}
    for path in sorted(glob.glob(prefix + '-*.warc.gz')):
        with open(path, 'rb') as f:
            result[os.path.basename(path)] = [
                (r.rec_type, r.rec_headers.get_header('WARC-Filename') or r.rec_headers.get_header('WARC-Target-URI'))
                for r in ArchiveIterator(f)
            ]
    return result


def write_all(series, records):
    async def main():
        for data in records:
            await series.write(data)
        await series.close()

    asyncio.run(main())


def test_no_file_without_records(tmp_path):
    write_all(WarcFileSeries(str(tmp_path / 'out'), {}), [])
    assert os.listdir(tmp_path) == []


def test_rotation_and_warcinfo_per_file(tmp_path):
    prefix = str(tmp_path / 'out')
    data = [record(f'https://example.com/{i}') for i in range(3)]
    # room for the warcinfo record and one record, not two
    series = WarcFileSeries(prefix, {'software': 'test'}, writer_subprefix='SUB', max_file_size=len(data[0]) * 2)
    write_all(series, data)

    assert records_by_file(prefix) == {
        f'out-SUB-00{i + 1}.warc.gz': [('warcinfo', f'out-SUB-00{i + 1}.warc.gz'),
                                       ('resource', f'https://example.com/{i}')]
        for i in range(3)
    }
    assert series.sequence == 3


def test_record_larger_than_the_size_limit_gets_a_file(tmp_path):
    prefix = str(tmp_path / 'out')
    write_all(WarcFileSeries(prefix, {}, max_file_size=1), [record('https://example.com/a')])
    assert list(records_by_file(prefix)) == ['out-001.warc.gz']


def test_shared_file_numbers(tmp_path):
    numbers = iter([5, 9])
    prefix = str(tmp_path / 'out')
    series = WarcFileSeries(prefix, {}, max_file_size=1, next_file_sequence=lambda current: next(numbers))
    write_all(series, [record('https://example.com/a'), record('https://example.com/b')])
    assert list(records_by_file(prefix)) == ['out-005.warc.gz', 'out-009.warc.gz']


def test_failure_aborts_the_current_file_only(tmp_path):
    prefix = str(tmp_path / 'out')
    series = WarcFileSeries(prefix, {}, max_file_size=1)
    original_write = LocalOutputFile.write

    writes = []

    async def write(self, data):
        writes.append(self.file_path)
        if writes.count(self.file_path) == 2 and self.file_path.endswith('-002.warc.gz'):  # after the warcinfo
            raise OSError('disk full')
        await original_write(self, data)

    async def main():
        await series.write(record('https://example.com/a'))
        try:
            await series.write(record('https://example.com/b'))
        except OSError:
            await series.abort()
            raise

    with patch.object(LocalOutputFile, 'write', write), pytest.raises(OSError, match='disk full'):
        asyncio.run(main())
    assert list(records_by_file(prefix)) == ['out-001.warc.gz']
