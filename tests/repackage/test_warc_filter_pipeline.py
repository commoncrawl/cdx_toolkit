"""The WARC fetch/write pipeline end to end without network: fake HTTP and HF reads, in-process "processes"."""
import asyncio
import contextlib
import glob
import os
from io import BytesIO
from unittest.mock import Mock, patch

import fsspec
import pytest
from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.repackage import multiprocess
from cdx_toolkit.repackage.sources.records import RecordsSource
from cdx_toolkit.repackage.warc_filter import WARCFilter
from cdx_toolkit.repackage.warc_utils import get_bytes_from_warc_record


def warc_record_bytes(url):
    record = WARCWriter(None).create_warc_record(url, 'resource', payload=BytesIO(b'content of ' + url.encode()))
    return get_bytes_from_warc_record(record, gzip=True)


CAPTURES = [
    {'url': f'https://example.com/{i}', 'filename': f'crawl-data/{i % 2}.warc.gz', 'offset': str(i), 'length': '1'}
    for i in range(4)
]


def fake_get(url, headers=None, **kwargs):
    return Mock(content=warc_record_bytes(url + '#' + headers['Range']))


def response_records(paths):
    records = []
    for path in sorted(paths):
        with fsspec.open(path, 'rb') as f:
            records += [r for r in ArchiveIterator(f) if r.rec_type == 'resource']
    return records


def test_reader_and_writer_errors_are_logged_and_skipped(tmp_path, caplog):
    calls = []

    def flaky_get(url, headers=None, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            raise ConnectionError('network down')
        return fake_get(url, headers)

    wf = WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={})
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=flaky_get):
        assert wf.filter() == 3

    assert 'WARC Reader 0 failed' in caplog.text
    assert len(response_records(glob.glob(str(tmp_path / 'out-*.warc.gz')))) == 3


def test_writer_error_is_logged(tmp_path, caplog):
    wf = WARCFilter(source=RecordsSource(CAPTURES[:1], 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={})
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get), \
            patch('cdx_toolkit.repackage.warc_filter.create_new_writer_with_header', side_effect=OSError('disk full')):
        assert wf.filter() == 0
    assert 'WARC writer failed' in caplog.text


def test_hf_reads_use_the_hf_reader(tmp_path):
    class FakeHfReader:
        async def ranged_get(self, url, offset, length, **kwargs):
            return warc_record_bytes(f'{url}#{offset}')

    @contextlib.asynccontextmanager
    async def fake_make_hf_reader():
        yield FakeHfReader()

    wf = WARCFilter(source=RecordsSource(CAPTURES, 'hf://buckets/commoncrawl/commoncrawl'),
                    prefix_path=str(tmp_path / 'out'), writer_info={},
                    warc_download_prefix='hf://buckets/commoncrawl/commoncrawl')
    with patch('cdx_toolkit.repackage.warc_filter.make_hf_reader', fake_make_hf_reader):
        assert wf.filter() == 4
    records = response_records(glob.glob(str(tmp_path / 'out-*.warc.gz')))
    assert records[0].rec_headers.get_header('WARC-Target-URI').startswith('hf://buckets/commoncrawl/commoncrawl/')


@pytest.mark.parametrize('uvloop_installed', [True, False])
def test_uvloop(monkeypatch, caplog, uvloop_installed):
    monkeypatch.setenv('CDXT_UVLOOP', '1')
    fake_uvloop = Mock(run=asyncio.run) if uvloop_installed else None
    monkeypatch.setitem(__import__('sys').modules, 'uvloop', fake_uvloop)
    wf = WARCFilter(source=RecordsSource([], None), prefix_path='/nonexistent/out', writer_info={})

    with caplog.at_level('INFO'):
        assert wf.filter() == 0
    assert ('Using uvloop event loop' if uvloop_installed else 'uvloop is not installed') in caplog.text


def test_no_aws_clients_without_s3():
    wf = WARCFilter(source=RecordsSource([], None), prefix_path='out', writer_info={},
                    warc_download_prefix='https://data.commoncrawl.org')
    assert asyncio.run(wf.get_aws_clients()) is None


class InlineExecutor:
    """ProcessPoolExecutor stand-in that runs the workers in this process (patches apply, coverage is measured)."""

    def __init__(self, max_workers, mp_context, initializer, initargs):
        initializer(*initargs)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def map(self, fn, items):
        return [fn(item) for item in items]


@pytest.mark.parametrize('remote', [False, True])
def test_multiprocess_repackage(tmp_path, remote):
    prefix = 'memory://mp-test/out/RUN' if remote else str(tmp_path / 'RUN')
    with patch.object(multiprocess, 'ProcessPoolExecutor', InlineExecutor), \
            patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get):
        n = multiprocess.run_multiprocess_repackage(
            source=RecordsSource(CAPTURES, 'https://data.example.org'), n_processes=3, readers_per_process=2,
            prefix_path=prefix, writer_info={'isPartOf': 'test'}, writer_subprefix='SUB',
            write_paths_as_metadata_records=None, log_every_n=1, aws_region_name='us-east-1', max_attempts=1,
            uvloop=False, max_file_size=1,
        )

    assert n == 4
    fs = fsspec.filesystem('memory')
    if remote:
        paths = ['memory://' + p for p in fs.glob('/mp-test/out/RUN-SUB-*.warc.gz')]
    else:
        paths = glob.glob(str(tmp_path / 'RUN-SUB-*.warc.gz'))
    try:
        # one file per record (--size 1), numbered by the shared counter
        assert sorted(os.path.basename(p) for p in paths) == [f'RUN-SUB-00{i}.warc.gz' for i in range(1, 5)]
        assert len(response_records(paths)) == 4
    finally:
        if remote:
            fs.rm('/mp-test', recursive=True)
