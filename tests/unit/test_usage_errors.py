"""Usage errors and small edge cases of `cdxt index` / `cdxt repackage` and their helpers."""
import asyncio
import io
import runpy
import sys
from unittest.mock import patch

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.indexes import IndexQuery, UsageError
from cdx_toolkit.indexes.cdx_api import CdxApiBackend
from cdx_toolkit.indexes.cdx_files import CdxFilesBackend, load_url_patterns
from cdx_toolkit.indexes.patterns import domain_entry_to_surts, url_pattern_to_surts
from cdx_toolkit.indexes.sql import DuckDbBackend, sql_timestamp
from cdx_toolkit.record_io import RecordWriter, import_pyarrow, read_records, write_records
from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.sql_base import quote_sql_string

from tests.conftest import TEST_DATA_PATH

CDX_PATH = str(TEST_DATA_PATH / 'repackage' / 'filtered_CC-MAIN-2024-30_cdx-00187.gz')


def test_python_m_cdx_toolkit_cli(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['cdxt', '--version'])
    with pytest.raises(SystemExit):
        runpy.run_module('cdx_toolkit.cli', run_name='__main__')
    assert capsys.readouterr().out.strip()


@pytest.mark.parametrize(
    'args, error',
    [
        (['index', 'cc', '--url', 'example.com/*', '-o', 'out.txt'], 'cannot detect the output format of out.txt'),
        (['index', CDX_PATH, '--domain', 'mnhn.fr', '--to', '2024-01'], 'invalid timestamp'),
        (['index', CDX_PATH, '--url', 'ex*ample.com/*'], '* can only be at the start or the end'),
        (['index', 'cc-url-index-s3', '--domain', 'example.com', '--to', '20241332'], "invalid timestamp '20241332'"),
        (["index", "/data/it's.parquet", '--domain', 'example.com'], 'invalid index location'),
    ],
)
def test_usage_errors(capsys, args, error):
    with pytest.raises(SystemExit) as e:
        main(args)
    assert e.value.code == 2
    assert error in capsys.readouterr().err


@pytest.mark.parametrize(
    'command', [['index', 'cc', '--url', 'x.org/*', '-o', 'x.parquet'], ['repackage', 'x.parquet']]
)
def test_parquet_without_pyarrow(capsys, command):
    with patch('cdx_toolkit.cli.index.import_pyarrow', side_effect=RuntimeError('needs pyarrow')), \
            patch('cdx_toolkit.cli.repackage.import_pyarrow', side_effect=RuntimeError('needs pyarrow')):
        with pytest.raises(SystemExit):
            main(command)
    assert 'needs pyarrow' in capsys.readouterr().err


def test_import_pyarrow_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, 'pyarrow', None)
    with pytest.raises(RuntimeError, match=r'cdx_toolkit\[parquet\]'):
        import_pyarrow()


def test_cdx_api_dry_run_and_cc_sort(capsys):
    main(['index', 'cc', '--domain', 'example.com', '--crawl', '3', '--limit', '5', '--dry-run'])
    assert capsys.readouterr().out == 'cc: url=example.com limit=5 matchType=domain\ncrawls: 3\n'

    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        CdxApiBackend('cc', cc_sort='ascending')._fetcher(IndexQuery(urls=['x']))
    assert fetcher.call_args.kwargs == {'source': 'cc', 'cc_sort': 'ascending'}


def test_cdx_files_edge_cases(tmp_path, capsys):
    with pytest.raises(UsageError, match='unknown --url-file-type'):
        CdxFilesBackend(CDX_PATH).validate(IndexQuery(url_file='u.txt', url_file_type='regex'))
    with pytest.raises(FileNotFoundError, match='no files found'):
        CdxFilesBackend(str(tmp_path / 'cdx-*.gz')).files()

    # rows of a URL file without the column are skipped (after the first one)
    path = tmp_path / 'urls.jsonl'
    path.write_text('{"url": "example.com/a"}\n{"other": 1}\n{"url": "example.com/b"}\n')
    assert load_url_patterns(str(path)) == ['example.com/a', 'example.com/b']

    # the time range excludes all lines; it is shown in the dry run
    assert CdxFilesBackend(CDX_PATH).count(IndexQuery(domains=['mnhn.fr'], to='2023')) == 0
    main(['index', CDX_PATH, '--domain', 'mnhn.fr', '--from', '2024', '--dry-run'])
    assert 'timestamps: 20240101000000 to -' in capsys.readouterr().out


def test_pattern_errors():
    with pytest.raises(UsageError):
        url_pattern_to_surts('ex*ample.com/*')
    with pytest.raises(UsageError, match='no wildcards'):
        domain_entry_to_surts('*.example.com')


def test_sql_helpers():
    with pytest.raises(UsageError):
        sql_timestamp('20241332')
    with pytest.raises(ValueError, match='control characters'):
        quote_sql_string('a\nb')
    with pytest.raises(UsageError):
        DuckDbBackend("x'.parquet")


def test_record_io_unknown_formats(tmp_path):
    with pytest.raises(ValueError, match='unknown format'):
        RecordWriter(io.StringIO(), 'xml')
    path = tmp_path / 'r.jsonl'
    path.write_text('{"url": "x"}\n')
    with pytest.raises(ValueError, match='unknown format'):
        list(read_records(str(path), 'xml'))


def test_parquet_batches_exactly_full(tmp_path, monkeypatch):
    pytest.importorskip('pyarrow')
    monkeypatch.setattr('cdx_toolkit.record_io.PARQUET_BATCH_SIZE', 2)
    path = str(tmp_path / 'r.parquet')
    assert write_records(iter([{'url': str(i)} for i in range(4)]), path, 'parquet') == 4
    assert [r['url'] for r in read_records(path)] == ['0', '1', '2', '3']


def test_hf_range_job_needs_a_reader():
    job = RangeJob(url='hf://buckets/ns/name/x.warc.gz', offset=0, length=1)
    with pytest.raises(ValueError, match='requires an hf_reader'):
        asyncio.run(job.ranged_get_bytes(1, 0))

    class Reader:
        async def ranged_get(self, url, offset, length, **kwargs):
            return b'x'

    assert asyncio.run(job.ranged_get_bytes(1, 0, hf_reader=Reader())) == b'x'
