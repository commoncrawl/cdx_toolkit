"""DuckDbSource with a local parquet file, and its connection setup with a fake connection (no network)."""
from unittest.mock import MagicMock, patch

import pytest

from cdx_toolkit.repackage.sources import duckdb as duckdb_source
from cdx_toolkit.repackage.sources.duckdb import DuckDbSource, _build_from_clause

from tests.conftest import requires_duckdb


def test_from_clause():
    assert _build_from_clause('s3://b/index/', ['CC-MAIN-2024-33']) == (
        "read_parquet(['s3://b/index/crawl=CC-MAIN-2024-33/subset=warc/*.parquet'], hive_partitioning=true)"
    )
    assert _build_from_clause('s3://b/index', None) == (
        "read_parquet(['s3://b/index/crawl=*/subset=warc/*.parquet'], hive_partitioning=true)"
    )


@requires_duckdb
def test_range_jobs_from_parquet(tmp_path):
    import duckdb

    path = tmp_path / 'index.parquet'
    duckdb.connect().execute(f'''
        COPY (SELECT * FROM (VALUES ('crawl-data/a.warc.gz', 10, 100, 'https://example.com/'),
                                    ('crawl-data/b.warc.gz', 20, 200, 'https://example.com/x'))
              AS t(warc_filename, warc_record_offset, warc_record_length, url))
        TO '{path}' (FORMAT parquet)
    ''')

    source = DuckDbSource(query=f"SELECT * FROM read_parquet('{path}')", warc_download_prefix='s3://commoncrawl')
    jobs = list(source.iter_range_jobs())
    assert [(j.url, j.offset, j.length, j.extra) for j in jobs] == [
        ('s3://commoncrawl/crawl-data/a.warc.gz', 10, 100, {'url': 'https://example.com/'}),
        ('s3://commoncrawl/crawl-data/b.warc.gz', 20, 200, {'url': 'https://example.com/x'}),
    ]

    with pytest.raises(ValueError, match='missing required columns'):
        list(DuckDbSource(query=f"SELECT url FROM read_parquet('{path}')").iter_range_jobs())


def test_without_duckdb():
    with patch.object(duckdb_source, '_HAS_DUCKDB', False), pytest.raises(RuntimeError, match=r'cdx_toolkit\[duckdb\]'):
        list(DuckDbSource(query='SELECT 1').iter_rows())


def test_connection_setup_and_hf_filesystem(monkeypatch):
    for name in ('AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_MAX_ATTEMPTS', 'CDXT_AWS_DEFAULTS'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr('cdx_toolkit.aws.user_aws_config', lambda: {})
    con = MagicMock()
    con.execute.return_value.description = [('n',)]
    con.execute.return_value.fetchmany.side_effect = [[(1,)], []]
    fake_duckdb = MagicMock(connect=MagicMock(return_value=con))
    hf_fs = MagicMock()

    fake_hf = MagicMock(HfFileSystem=MagicMock(return_value=hf_fs))
    query = "SELECT count(*) AS n FROM read_parquet('hf://buckets/x/y/*.parquet')"
    with patch.object(duckdb_source, '_HAS_DUCKDB', True), \
            patch.object(duckdb_source, 'duckdb', fake_duckdb, create=True), \
            patch.dict('sys.modules', {'huggingface_hub': fake_hf}):
        rows = list(DuckDbSource(query=query).iter_rows())

    assert rows == [{'n': 1}]
    statements = [c.args[0] for c in con.execute.call_args_list]
    assert "SET TimeZone='UTC';" in statements
    # Common Crawl's recommended S3 settings (cdx_toolkit.aws) and a timeout of 2 minutes
    for statement in ("SET s3_region='us-east-1';", "SET s3_url_style='vhost';", 'SET http_retries=99999;',
                      'SET http_retry_wait_ms=1000;', 'SET http_retry_backoff=1;', 'SET http_timeout=120;'):
        assert statement in statements
    assert "CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');" in statements
    con.register_filesystem.assert_called_once_with(hf_fs)
    con.close.assert_called_once()
