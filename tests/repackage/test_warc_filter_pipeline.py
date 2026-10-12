"""The WARC fetch/write pipeline end to end without network: fake HTTP and HF reads, in-process "processes"."""
import asyncio
import contextlib
import glob
import io
import gzip
import itertools
import os
import threading
from unittest.mock import Mock, patch

import fsspec
import pytest
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.repackage import multiprocess
from cdx_toolkit.repackage.data_classes import RangeJob, RepackageResult
from cdx_toolkit.repackage.http_range import RangeError
from cdx_toolkit.repackage.local_writer import LocalFileWriter
from cdx_toolkit.repackage.records_source import RecordsSource
from cdx_toolkit.repackage.warc_filter import WARCFilter
from tests.fake_warc_server import FakeWarcServer
from tests.conftest import requires_aioboto3


def make_server():
    """Four captures in two WARC files."""
    server = FakeWarcServer('https://data.example.org')
    captures = []
    for i in range(4):
        captures += server.add_captures(f'crawl-data/{i % 2}.warc.gz', [f'https://example.com/{i}'])
    return server, captures


SERVER, CAPTURES = make_server()
fake_get = SERVER.get


def response_records(paths):
    records = []
    for path in sorted(paths):
        with fsspec.open(path, 'rb') as f:
            records += [r for r in ArchiveIterator(f) if r.rec_type == 'resource']
    return records


def test_failed_captures_are_counted_and_the_others_written(tmp_path):
    calls = []

    def flaky_get(url, headers=None, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            raise ConnectionError('network down')
        return fake_get(url, headers)

    wf = WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={})
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=flaky_get):
        assert wf.filter() == RepackageResult(requested=4, written=3, failed=1)
    assert len(response_records(glob.glob(str(tmp_path / 'out-*.warc.gz')))) == 3


@pytest.mark.parametrize('response', [
    Mock(status_code=200, headers={}, content=b'x' * 100),  # range ignored
    Mock(status_code=301, headers={'Location': 'https://elsewhere.example.org/'}, content=b''),  # redirect
    Mock(status_code=206, headers={'Content-Range': 'bytes 0-0/100'}, content=b'x'),  # truncated / wrong range
    'wrong-range',
])
def test_invalid_range_responses_are_not_written(tmp_path, response):
    capture = CAPTURES[1]  # offset > 0
    offset, length = int(capture['offset']), int(capture['length'])
    if response == 'wrong-range':
        response = Mock(status_code=206, headers={'Content-Range': f'bytes {offset + 1}-{offset + length}/9999'},
                        content=b'x' * length)
    wf = WARCFilter(source=RecordsSource([capture], 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={})
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', return_value=response):
        assert wf.filter() == RepackageResult(requested=1, written=0, failed=1)
    assert glob.glob(str(tmp_path / 'out-*')) == []


def test_a_slow_http_read_does_not_block_the_other_readers_and_the_writer(tmp_path):
    """The first read is held until the second capture has been written."""
    second_written = threading.Event()
    original_write = LocalFileWriter.write

    def get(url, headers=None, **kwargs):
        if url.endswith(CAPTURES[0]['filename']):
            assert second_written.wait(timeout=5), 'the second capture was not written while the first was read'
        return fake_get(url, headers)

    async def write(self, data):
        await original_write(self, data)
        if b'https://example.com/1' in gzip.decompress(data):
            second_written.set()

    wf = WARCFilter(source=RecordsSource(CAPTURES[:2], 'https://data.example.org', sort=False),
                    prefix_path=str(tmp_path / 'out'), writer_info={}, n_parallel_readers=2)
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=get), \
            patch.object(LocalFileWriter, 'write', write):
        assert wf.filter() == RepackageResult(requested=2, written=2)
    records = response_records(glob.glob(str(tmp_path / 'out-*.warc.gz')))
    assert [r.rec_headers.get_header('WARC-Target-URI') for r in records] == [CAPTURES[1]['url'], CAPTURES[0]['url']]


def test_writer_error_stops_the_pipeline_and_removes_the_file(tmp_path):
    wf = WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={}, max_file_size=1)
    writes = []

    original_write = LocalFileWriter.write

    async def failing_write(self, data):
        writes.append(data)
        if len(writes) == 4:  # warcinfo + record of the first file, warcinfo + record of the second
            raise OSError('disk full')
        await original_write(self, data)

    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get), \
            patch.object(LocalFileWriter, 'write', failing_write), pytest.raises(OSError, match='disk full'):
        wf.filter()
    # the first file is complete, the second was removed
    assert [os.path.basename(p) for p in glob.glob(str(tmp_path / 'out-*'))] == ['out-001.warc.gz']


def test_rotation_by_size(tmp_path):
    wf = WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={}, max_file_size=1)
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get):
        assert wf.filter().written == 4
    paths = sorted(glob.glob(str(tmp_path / 'out-*.warc.gz')))
    assert [os.path.basename(p) for p in paths] == [f'out-00{i}.warc.gz' for i in range(1, 5)]
    assert all(len(response_records([p])) == 1 for p in paths)


def test_cancellation_with_a_full_queue_and_a_blocked_reader(tmp_path):
    """Cancelling must not wait for queue space: the producer thread stops and the current file is removed."""
    source_done = threading.Event()

    class EndlessSource:
        def iter_range_jobs(self):
            try:
                for i in itertools.count():
                    yield RangeJob(url=f'https://data.example.org/{i}', offset=0, length=1)
            finally:
                source_done.set()

    reads = []

    async def blocking_read(self, *args, **kwargs):
        reads.append(self.url)
        if len(reads) > 1:
            await asyncio.Event().wait()  # blocks forever
        return b'x'

    wf = WARCFilter(source=EndlessSource(), prefix_path=str(tmp_path / 'out'), writer_info={},
                    range_jobs_queue_size=2)

    async def run():
        task = asyncio.create_task(wf.filter_async())
        while len(reads) < 2:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)  # the queue fills up, the producer waits
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)

    def main():
        with patch.object(RangeJob, 'ranged_get_bytes', blocking_read):
            asyncio.run(run())

    thread = threading.Thread(target=main, daemon=True)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive(), 'shutdown did not finish'
    assert source_done.wait(timeout=1), 'the producer thread is still waiting'
    assert glob.glob(str(tmp_path / 'out-*')) == []


def test_hf_reads_use_the_hf_reader(tmp_path):
    wf = WARCFilter(source=RecordsSource(CAPTURES, 'hf://buckets/commoncrawl/commoncrawl'),
                    prefix_path=str(tmp_path / 'out'), writer_info={})
    with patch('cdx_toolkit.repackage.warc_filter.make_hf_reader', fake_make_hf_reader):
        assert wf.filter().written == 4
    records = response_records(glob.glob(str(tmp_path / 'out-*.warc.gz')))
    assert sorted(r.rec_headers.get_header('WARC-Target-URI') for r in records) == sorted(c['url'] for c in CAPTURES)


@pytest.mark.parametrize('uvloop_installed', [True, False])
def test_uvloop(monkeypatch, caplog, uvloop_installed):
    monkeypatch.setenv('CDXT_UVLOOP', '1')
    fake_uvloop = Mock(run=asyncio.run) if uvloop_installed else None
    monkeypatch.setitem(__import__('sys').modules, 'uvloop', fake_uvloop)
    wf = WARCFilter(source=RecordsSource([], None), prefix_path='/nonexistent/out', writer_info={})

    with caplog.at_level('INFO'):
        assert wf.filter() == RepackageResult()
    assert ('Using uvloop event loop' if uvloop_installed else 'uvloop is not installed') in caplog.text


class FakeS3Client:
    """aioboto3 S3 client stand-in that serves the files of SERVER and stores multipart uploads."""

    def __init__(self):
        self.parts = {}
        self.objects = {}

    async def create_multipart_upload(self, Bucket, Key):
        return {'UploadId': Key}

    async def upload_part(self, Key, PartNumber, Body, **kwargs):
        self.parts[(Key, PartNumber)] = Body
        return {'ETag': str(PartNumber)}

    async def complete_multipart_upload(self, Bucket, Key, MultipartUpload, **kwargs):
        parts = [self.parts[(Key, part['PartNumber'])] for part in MultipartUpload['Parts']]
        self.objects[f's3://{Bucket}/{Key}'] = b''.join(parts)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get_object(self, Bucket, Key, Range):
        start, end = (int(n) for n in Range[len('bytes='):].split('-'))

        class Body:
            async def read(self):
                return SERVER.files[Key][start:end + 1]

        return {'Body': Body()}


class FakeHfReader:
    async def ranged_get(self, url, offset, length, **kwargs):
        return SERVER.files[url.split('/', 5)[-1]][offset:offset + length]


@contextlib.asynccontextmanager
async def fake_make_hf_reader():
    yield FakeHfReader()


def mixed_scheme_captures():
    """The captures with full s3:// and hf:// filenames, and a relative one (default download prefix)."""
    s3, hf, relative = CAPTURES[0], CAPTURES[1], CAPTURES[2]
    return [dict(s3, filename='s3://commoncrawl/' + s3['filename']),
            dict(hf, filename='hf://buckets/commoncrawl/commoncrawl/' + hf['filename']), relative]


@requires_aioboto3
@pytest.mark.parametrize('processes', [1, 2])
def test_full_s3_and_hf_filenames_get_their_readers(tmp_path, processes):
    source = RecordsSource(mixed_scheme_captures(), 'https://data.example.org')
    with patch('aioboto3.Session') as session, \
            patch('cdx_toolkit.repackage.warc_filter.make_hf_reader', fake_make_hf_reader), \
            patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get), \
            patch.object(multiprocess, 'ProcessPoolExecutor', InlineExecutor):
        session.return_value.client.return_value = FakeS3Client()
        if processes == 1:
            result = WARCFilter(source=source, prefix_path=str(tmp_path / 'out'), writer_info={}).filter()
        else:
            result = multiprocess.run_multiprocess_repackage(
                source=source, n_processes=processes, readers_per_process=1, prefix_path=str(tmp_path / 'out'),
                writer_info={}, writer_subprefix=None, write_paths_as_metadata_records=None, log_every_n=0,
                max_attempts=1, uvloop=False,
            )
    assert result == RepackageResult(requested=3, written=3)
    records = response_records(glob.glob(str(tmp_path / 'out-*.warc.gz')))
    assert sorted(r.rec_headers.get_header('WARC-Target-URI') for r in records) == \
        sorted(c['url'] for c in CAPTURES[:3])


@requires_aioboto3
def test_s3_output_with_metadata_records(tmp_path):
    metadata = tmp_path / 'urls.txt'
    metadata.write_text('https://example.com/\n')
    s3 = FakeS3Client()
    with patch('aioboto3.Session') as session, \
            patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get):
        session.return_value.client.return_value = s3
        result = WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path='s3://bucket/out',
                            writer_info={}, write_paths_as_metadata_records=[str(metadata)]).filter()
    assert result.written == 4
    data = s3.objects['s3://bucket/out-001.warc.gz']
    records = [(r.rec_type, r.content_stream().read()) for r in ArchiveIterator(io.BytesIO(data))]
    assert [rec_type for rec_type, _ in records] == ['warcinfo', 'metadata'] + ['resource'] * 4
    assert records[1][1] == b'https://example.com/\n'


def test_failing_metadata_record_removes_the_file(tmp_path):
    wf = WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                    writer_info={}, write_paths_as_metadata_records=[str(tmp_path / 'missing.txt')])
    with patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get), \
            pytest.raises(FileNotFoundError):
        wf.filter()
    assert glob.glob(str(tmp_path / 'out-*')) == []


def test_s3_reads_check_the_length():
    class Readers:
        async def s3(self):
            class Client:
                async def get_object(self, **kwargs):
                    class Body:
                        async def read(self):
                            return b'short'
                    return {'Body': Body()}
            return Client()

    job = RangeJob(url='s3://commoncrawl/crawl-data/a.warc.gz', offset=0, length=10)
    with pytest.raises(RangeError, match='requested 10 bytes'):
        asyncio.run(job.ranged_get_bytes(1, 0, readers=Readers()))
    with pytest.raises(ValueError, match='requires readers'):
        asyncio.run(job.ranged_get_bytes(1, 0))


@requires_aioboto3
def test_no_s3_client_without_s3(tmp_path):
    with patch('aioboto3.Session') as session, \
            patch('cdx_toolkit.repackage.data_classes.myrequests_get', side_effect=fake_get):
        WARCFilter(source=RecordsSource(CAPTURES, 'https://data.example.org'), prefix_path=str(tmp_path / 'out'),
                   writer_info={}).filter()
    session.assert_not_called()


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

    assert n == RepackageResult(requested=4, written=4)
    fs = fsspec.filesystem('memory')
    if remote:
        paths = ['memory://' + p for p in fs.glob('/mp-test/out/RUN-SUB-*.warc.gz')]
    else:
        paths = glob.glob(str(tmp_path / 'RUN-SUB-*.warc.gz'))
    try:
        # one file per record (--size 1), numbered by the shared counter
        assert sorted(os.path.basename(p) for p in paths) == [f'RUN-SUB-00{i}.warc.gz' for i in range(1, 5)]
        assert len(response_records(paths)) == 4
        for path in paths:  # the warcinfo record names the file it is in
            with fsspec.open(path, 'rb') as f:
                warcinfo = next(iter(ArchiveIterator(f)))
            assert warcinfo.rec_headers.get_header('WARC-Filename') == os.path.basename(path)
    finally:
        if remote:
            fs.rm('/mp-test', recursive=True)
