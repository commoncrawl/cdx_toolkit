"""DuckDB queries of a local parquet file, and the connection setup with a fake connection (no network)."""
from unittest.mock import MagicMock, patch

import pytest

from cdx_toolkit.indexes.backends import duckdb as duckdb_source
from cdx_toolkit.indexes.backends.duckdb import build_from_clause, iter_duckdb_rows

from tests.conftest import requires_duckdb


def test_from_clause():
    assert build_from_clause('s3://b/index/', ['CC-MAIN-2024-33']) == (
        "read_parquet(['s3://b/index/crawl=CC-MAIN-2024-33/subset=warc/*.parquet'], hive_partitioning=true)"
    )
    assert build_from_clause('s3://b/index', None) == (
        "read_parquet(['s3://b/index/crawl=*/subset=warc/*.parquet'], hive_partitioning=true)"
    )


@requires_duckdb
def test_rows_from_parquet(tmp_path):
    import duckdb

    path = tmp_path / 'index.parquet'
    duckdb.connect().execute(f'''
        COPY (SELECT * FROM (VALUES ('crawl-data/a.warc.gz', 10), ('crawl-data/b.warc.gz', 20))
              AS t(warc_filename, warc_record_offset))
        TO '{path}' (FORMAT parquet)
    ''')
    assert list(iter_duckdb_rows(f"SELECT * FROM read_parquet('{path}')")) == [
        {'warc_filename': 'crawl-data/a.warc.gz', 'warc_record_offset': 10},
        {'warc_filename': 'crawl-data/b.warc.gz', 'warc_record_offset': 20},
    ]


def test_without_duckdb():
    with patch.object(duckdb_source, '_HAS_DUCKDB', False), pytest.raises(RuntimeError, match=r'cdx_toolkit\[duckdb\]'):
        list(iter_duckdb_rows('SELECT 1'))


def run_with_fake_connection(query):
    """The rows of the query, the executed statements and the connection."""
    con = MagicMock()
    con.execute.return_value.description = [('n',)]
    con.execute.return_value.fetchmany.side_effect = [[(1,)], []]
    fake_duckdb = MagicMock(connect=MagicMock(return_value=con))
    with patch.object(duckdb_source, '_HAS_DUCKDB', True), \
            patch.object(duckdb_source, 'duckdb', fake_duckdb, create=True):
        rows = list(iter_duckdb_rows(query))
    return rows, [c.args[0] for c in con.execute.call_args_list], con


@pytest.mark.parametrize('query, httpfs, s3', [
    ("SELECT 1 AS n FROM read_parquet('/data/index/*.parquet')", False, False),
    ("SELECT 1 AS n FROM read_parquet('https://data.commoncrawl.org/x.parquet')", True, False),
    ("SELECT 1 AS n FROM read_parquet('s3://commoncrawl/x.parquet')", True, True),
])
def test_extensions_and_credentials_only_when_needed(monkeypatch, query, httpfs, s3):
    monkeypatch.setattr('cdx_toolkit.aws.user_aws_config', lambda: {})
    rows, statements, _ = run_with_fake_connection(query)
    assert rows == [{'n': 1}]
    assert ('INSTALL httpfs; LOAD httpfs;' in statements) == httpfs
    assert ('SET http_timeout=120;' in statements) == httpfs
    assert ('INSTALL aws; LOAD aws;' in statements) == s3
    assert any(stmt.startswith('SET s3_region=') for stmt in statements) == s3
    assert statements[-1] == query


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
    query = "SELECT count(*) AS n FROM read_parquet(['hf://buckets/x/y/*.parquet', 's3://commoncrawl/x.parquet'])"
    with patch.object(duckdb_source, '_HAS_DUCKDB', True), \
            patch.object(duckdb_source, 'duckdb', fake_duckdb, create=True), \
            patch.dict('sys.modules', {'huggingface_hub': fake_hf}):
        rows = list(iter_duckdb_rows(query))

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


@pytest.mark.parametrize('output', ['index.parquet', 'idx/index.parquet', 'idx/new.parquet'])
def test_output_must_not_overwrite_the_index(tmp_path, monkeypatch, capsys, output):
    from cdx_toolkit.cli import main

    monkeypatch.chdir(tmp_path)
    (tmp_path / 'idx').mkdir()
    for path in ('index.parquet', 'idx/index.parquet'):
        (tmp_path / path).write_bytes(b'PAR1')
    location = 'idx/' if output.startswith('idx/') else 'index.parquet'
    with pytest.raises(SystemExit) as e:
        main(['index', location, '--index-backend', 'duckdb', '--domain', 'example.com', '-o', output])
    assert e.value.code == 2
    assert 'would overwrite the input' in capsys.readouterr().err
    assert (tmp_path / 'index.parquet').read_bytes() == b'PAR1'
