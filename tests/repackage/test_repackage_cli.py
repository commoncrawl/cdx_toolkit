"""End-to-end tests of `cdxt repackage` with the async WARC fetcher.

The captures are the first records of a Common Crawl CDX file (a CDXJ file); the WARC
records are fetched from data.commoncrawl.org or s3://commoncrawl, which are mocked with
recorded records (see tests/conftest.py).
"""
import gzip
import multiprocessing
import os
from typing import List, Optional

import fsspec
import pytest
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.cli import main
from cdx_toolkit.record_io import parse_cdxj_line, read_records, write_records

from tests.conftest import InlineExecutor, requires_aws_s3, TEST_DATA_PATH


pytestmark = pytest.mark.usefixtures('mock_data_commoncrawl')


CDX_PATH = TEST_DATA_PATH / 'repackage' / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'
METADATA_RECORD_PATH = TEST_DATA_PATH / 'repackage/url_list.txt'


def write_captures(tmpdir, n: int = 10, name: str = 'captures.cdxj', s3_env=None) -> str:
    """The first n captures of the CDX fixture (with their offsets in the mocked s3://commoncrawl, if given)."""
    path = os.path.join(str(tmpdir), name)
    with gzip.open(CDX_PATH, 'rt') as f_in:
        lines = [f_in.readline() for _ in range(n)]
    if s3_env:
        write_records([s3_env.capture(parse_cdxj_line(line)) for line in lines], path, 'cdxj')
    else:
        with open(path, 'w') as f_out:
            f_out.writelines(lines)
    return path


def read_warc(warc_path):
    records = {'warcinfo': [], 'response': [], 'metadata': []}
    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            content = record.content_stream().read().decode('utf-8', errors='ignore')
            records.setdefault(record.rec_type, []).append((record, content))
    return records


def assert_repackage(
    tmpdir,
    warc_download_prefix: str,
    base_prefix,
    extra_args: Optional[List[str]] = None,
    warc_filename: str = 'TEST_warc_by_index-001.warc.gz',
    s3_env=None,
):
    base_prefix = str(base_prefix)
    main(
        args=[
            '-v',
            'repackage',
            write_captures(tmpdir, s3_env=s3_env if warc_download_prefix.startswith('s3://') else None),
            '--no-sort',  # keep the order of the CDX file
            '--add-metadata-record',
            str(METADATA_RECORD_PATH),
            f'--prefix={base_prefix}/TEST_warc_by_index',
            '--creator=foo',
            '--operator=bob',
            f'--warc-download-prefix={warc_download_prefix}',
        ]
        + (extra_args or [])
    )

    # with --processes, each worker writes its own WARC files: read the whole series
    fs, fs_prefix = fsspec.url_to_fs(f'{base_prefix}/TEST_warc_by_index')
    warc_paths = [fs.unstrip_protocol(p) for p in sorted(fs.glob(fs_prefix + '-*.warc.gz'))]
    assert warc_paths[0].endswith('/' + warc_filename)
    records = {'warcinfo': [], 'response': [], 'metadata': []}
    for warc_path in warc_paths:
        file_records = read_warc(warc_path)
        # each file is a complete WARC whose warcinfo record names the file itself
        (info_record, _), = file_records['warcinfo']
        assert info_record.rec_headers.get('WARC-Filename') == warc_path.rsplit('/', 1)[-1]
        for rec_type, recs in file_records.items():
            records[rec_type] += recs

    assert len(records['response']) == 10, 'Invalid record count'

    assert len(records['warcinfo']) == len(warc_paths)
    info_record, info_content = records['warcinfo'][0]
    assert info_record.rec_headers.get('WARC-Filename') == warc_filename
    assert 'operator: bob' in info_content, 'Invalid info record'

    response_contents = [content for _, content in records['response']]
    if any(arg.startswith('--processes') for arg in extra_args or []):
        # the captures are sharded across processes
        assert any('Catalogue en ligne Mission de France' in c for c in response_contents)
    else:
        assert 'Catalogue en ligne Mission de France' in response_contents[0], 'Invalid response content'
        assert 'dojo/dijit/themes/tundra/tundra' in response_contents[9], 'Invalid response content'

    # one metadata record per WARC file
    assert len(records['metadata']) == len(warc_paths)
    metadata_record, metadata_content = records['metadata'][0]
    assert metadata_content[:10] == 'example.co', 'Invalid metadata record'
    assert metadata_record.rec_headers.get('WARC-Payload-Digest') == 'sha1:VXA2A5YUS3TAY36AUO6MACRMNOH5RXG2'
    with open(METADATA_RECORD_PATH, 'rb') as f:
        assert metadata_record.length == len(f.read())


def test_repackage_over_http(tmpdir):
    assert_repackage(tmpdir, 'https://data.commoncrawl.org', base_prefix=tmpdir)


def test_repackage_over_http_with_parallel_readers(tmpdir):
    assert_repackage(tmpdir, 'https://data.commoncrawl.org', base_prefix=tmpdir, extra_args=['--parallel-readers=3'])


def test_repackage_over_http_with_processes(tmpdir, monkeypatch, mock_data_commoncrawl):
    if mock_data_commoncrawl is not None:
        # the HTTP mock only applies in this process
        monkeypatch.setattr('cdx_toolkit.repackage.multiprocess.process_pool', InlineExecutor)
    assert_repackage(tmpdir, 'https://data.commoncrawl.org', base_prefix=tmpdir, extra_args=['--processes=2'])


@pytest.mark.parametrize('fmt', ['csv', 'tsv', 'jsonl'])
def test_repackage_input_formats(tmpdir, fmt):
    """Captures written by `cdxt index` in any format (here: with the columnar index column names for csv)."""
    captures = list(read_records(write_captures(tmpdir)))
    fields = ['url', 'filename', 'offset', 'length']
    if fmt == 'csv':
        for c in captures:
            c['warc_filename'], c['warc_record_offset'], c['warc_record_length'] = (
                c.pop('filename'), c.pop('offset'), c.pop('length')
            )
        fields = ['url', 'warc_filename', 'warc_record_offset', 'warc_record_length']
    path = os.path.join(str(tmpdir), f'captures.{fmt}')
    write_records(captures, path, fmt, fields)

    main(args=['repackage', path, f'--prefix={tmpdir}/TEST'])
    main(args=['repackage', path, f'--input-format={fmt}', f'--prefix={tmpdir}/TEST2'])

    for prefix in ('TEST', 'TEST2'):
        responses = read_warc(os.path.join(str(tmpdir), f'{prefix}-001.warc.gz'))['response']
        assert len(responses) == 10


def test_repackage_subprefix_and_metadata(tmpdir):
    main(
        args=[
            'repackage',
            write_captures(tmpdir, n=1),
            f'--prefix={tmpdir}/TEST',
            '--subprefix=SUB',
            '--creator=test_creator',
            '--operator=test_operator',
            '--is-part-of=PART',
            '--description=DESC',
        ]
    )

    (_, info), = read_warc(os.path.join(str(tmpdir), 'TEST-SUB-001.warc.gz'))['warcinfo']
    assert 'creator: test_creator' in info
    assert 'operator: test_operator' in info
    assert 'isPartOf: PART' in info
    assert 'description: DESC' in info


def test_repackage_without_creator_operator(tmpdir):
    main(args=['repackage', write_captures(tmpdir, n=1), f'--prefix={tmpdir}/TEST_NO_META'])

    (_, info), = read_warc(os.path.join(str(tmpdir), 'TEST_NO_META-001.warc.gz'))['warcinfo']
    assert 'creator:' not in info
    assert 'operator:' not in info


def test_repackage_glob_input(tmpdir):
    write_captures(tmpdir, n=1, name='a.cdxj')
    write_captures(tmpdir, n=2, name='b.cdxj')

    main(args=['repackage', f'{tmpdir}/*.cdxj', f'--prefix={tmpdir}/TEST'])

    assert len(read_warc(os.path.join(str(tmpdir), 'TEST-001.warc.gz'))['response']) == 3


def test_repackage_no_input_files_found(tmpdir, capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(args=['repackage', f'{tmpdir}/nonexistent-*.gz', f'--prefix={tmpdir}/TEST'])

    assert exc_info.value.code == 2
    assert 'no files found' in capsys.readouterr().err


@pytest.mark.parametrize('option', ['--processes', '--parallel-readers'])
@pytest.mark.parametrize('value', ['0', '-1', 'x'])
def test_repackage_rejects_non_positive_counts(tmpdir, capsys, option, value):
    with pytest.raises(SystemExit) as exc_info:
        main(args=['repackage', write_captures(tmpdir, n=1), f'--prefix={tmpdir}/TEST', option, value])
    assert exc_info.value.code == 2
    assert 'positive integer' in capsys.readouterr().err


def test_repackage_rejects_an_existing_series(tmpdir, capsys):
    """A second run with the same prefix would overwrite the first one's files."""
    main(args=['repackage', write_captures(tmpdir, n=1), f'--prefix={tmpdir}/TEST'])
    first = read_warc(os.path.join(str(tmpdir), 'TEST-001.warc.gz'))['response']

    with pytest.raises(SystemExit) as exc_info:
        main(args=['repackage', write_captures(tmpdir, n=2), f'--prefix={tmpdir}/TEST'])

    assert exc_info.value.code == 2
    assert 'exist already' in capsys.readouterr().err
    assert read_warc(os.path.join(str(tmpdir), 'TEST-001.warc.gz'))['response'][0][1] == first[0][1]
    assert not os.path.exists(os.path.join(str(tmpdir), 'TEST-002.warc.gz'))
    # another subprefix is another series
    main(args=['repackage', write_captures(tmpdir, n=2), f'--prefix={tmpdir}/TEST', '--subprefix=B'])
    assert len(read_warc(os.path.join(str(tmpdir), 'TEST-B-001.warc.gz'))['response']) == 2


@requires_aws_s3
def test_repackage_rejects_an_existing_series_on_s3(tmpdir, s3_tmpdir):
    main(args=['repackage', write_captures(tmpdir, n=1), f'--prefix={s3_tmpdir}/TEST'])
    with pytest.raises(SystemExit) as exc_info:
        main(args=['repackage', write_captures(tmpdir, n=1), f'--prefix={s3_tmpdir}/TEST'])
    assert exc_info.value.code == 2


@requires_aws_s3
def test_repackage_over_s3(tmpdir, s3_env):
    assert_repackage(tmpdir, 's3://commoncrawl', base_prefix=tmpdir, s3_env=s3_env)


@requires_aws_s3
def test_repackage_over_s3_to_s3(tmpdir, s3_tmpdir, s3_env):
    assert_repackage(tmpdir, 's3://commoncrawl', base_prefix=s3_tmpdir, s3_env=s3_env)


@requires_aws_s3
def test_repackage_over_s3_to_s3_with_parallel_readers(tmpdir, s3_tmpdir, s3_env):
    assert_repackage(
        tmpdir, 's3://commoncrawl', base_prefix=s3_tmpdir, extra_args=['--parallel-readers=3', '--is-part-of=foobar'],
        s3_env=s3_env,
    )


@requires_aws_s3
def test_repackage_over_http_to_s3_with_parallel_readers(tmpdir, s3_tmpdir):
    assert_repackage(tmpdir, 'https://data.commoncrawl.org', base_prefix=s3_tmpdir, extra_args=['--parallel-readers=3'])


@requires_aws_s3
def test_repackage_over_s3_to_local_with_processes(tmpdir, s3_env, monkeypatch):
    # the worker processes use the mocked S3 too: it is configured with environment variables, which a spawned
    # process gets from this process (a forkserver, the default since Python 3.14 on Linux, could have been
    # started before, with other environment variables, e.g. real AWS credentials)
    spawn = multiprocessing.get_context('spawn')
    monkeypatch.setattr('cdx_toolkit.repackage.multiprocess.multiprocessing.get_context', lambda *args: spawn)
    assert_repackage(tmpdir, 's3://commoncrawl', base_prefix=tmpdir, extra_args=['--processes=2'], s3_env=s3_env)
