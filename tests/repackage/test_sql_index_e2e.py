"""End-to-end tests of the SQL indexes (DuckDB, Athena): `cdxt index ... | cdxt repackage`.

By default, DuckDB queries a local columnar index (the local_url_index fixture) and the WARC records are
served by the mocked data.commoncrawl.org (see tests/conftest.py). With CDXT_LIVE_TESTS=1, the live tests
query Common Crawl's index on S3 (DuckDB) or with Athena, only a single crawl partition for a single host
(cheap, partition-pruned -- never an all-crawls scan), and fetch the WARC records over HTTP.
"""
import json
import os

import fsspec
import pytest
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.cli import main
from tests.conftest import (
    LOCAL_INDEX_CRAWL, LOCAL_INDEX_HOST, TEST_ATHENA_S3_LOCATION, mock_response_from_jsonl, requires_aws_athena,
    requires_duckdb, requires_live_aws,
)


CRAWL = 'CC-MAIN-2026-17'
HOST = 'commoncrawl.org'
ATHENA = f'athena://athena.us-east-1.amazonaws.com/ccindex?S3OutputLocation={TEST_ATHENA_S3_LOCATION}'


@pytest.fixture(autouse=True)
def crawl_list_cache(tmp_path, monkeypatch):
    """--crawl is resolved with the list of crawls; cache it apart from the list of other tests."""
    monkeypatch.setattr('cdx_toolkit.commoncrawl.CACHE_DIR', str(tmp_path / 'cache') + '/')


@pytest.fixture
def mocked(mock_data_commoncrawl):
    """The mocked data.commoncrawl.org and the recorded list of crawls (for --crawl)."""
    if mock_data_commoncrawl is not None:
        mock_response_from_jsonl('test_get_cc_endpoints', 'test_cc')


def _index_and_repackage(tmpdir, index_args, host=HOST):
    captures_path = os.path.join(str(tmpdir), 'captures.jsonl')
    main(args=['index'] + index_args + ['-o', captures_path])

    with open(captures_path) as f:
        captures = [json.loads(line) for line in f]
    assert len(captures) > 0, 'expected at least one capture for the host/crawl'
    assert all(host in c['url'] for c in captures)

    base_prefix = str(tmpdir)
    main(args=['repackage', captures_path, f'--prefix={base_prefix}/TEST_sql'])

    response_count = 0
    with fsspec.open(os.path.join(base_prefix, 'TEST_sql-001.warc.gz'), 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                response_count += 1
                target = record.rec_headers.get_header('WARC-Target-URI') or ''
                assert host in target, f'unexpected target URI: {target}'

    assert response_count == len(captures), 'every capture should yield a response record'
    return captures


@requires_duckdb
@pytest.mark.usefixtures('mocked')
def test_index_duckdb_local_e2e(tmpdir, local_url_index):
    captures = _index_and_repackage(
        tmpdir, [str(local_url_index), '--index-backend', 'duckdb', '--crawl', LOCAL_INDEX_CRAWL,
                 '--hostname', LOCAL_INDEX_HOST], host=LOCAL_INDEX_HOST,
    )
    # all captures of the host in the crawl, not those of the other crawl
    assert len(captures) == 10
    assert {c['crawl'] for c in captures} == {LOCAL_INDEX_CRAWL}


@requires_duckdb
@pytest.mark.usefixtures('mocked')
def test_index_duckdb_local_domain_e2e(tmpdir, local_url_index):
    captures = _index_and_repackage(
        tmpdir, [str(local_url_index), '--index-backend', 'duckdb', '--crawl', LOCAL_INDEX_CRAWL,
                 '--domain', 'missiondefrance.fr', '--limit', '5'], host=LOCAL_INDEX_HOST,
    )
    assert len(captures) == 5


@requires_duckdb
@pytest.mark.usefixtures('mocked')
def test_index_duckdb_local_sql_e2e(tmpdir, local_url_index):
    sql = os.path.join(str(tmpdir), 'query.sql')
    with open(sql, 'w') as f:
        f.write(f"""
            SELECT url, warc_filename, warc_record_offset, warc_record_length, content_languages
            FROM read_parquet('{local_url_index}/crawl={LOCAL_INDEX_CRAWL}/subset=warc/*.parquet')
            WHERE url_host_name = '{LOCAL_INDEX_HOST}'
            LIMIT 3
        """)
    captures = _index_and_repackage(tmpdir, [str(local_url_index), '--index-backend', 'duckdb', '--sql', sql,
                                             '--yes'], host=LOCAL_INDEX_HOST)
    assert len(captures) == 3
    assert {c['languages'] for c in captures} == {'fra'}


@requires_aws_athena
def test_index_athena_e2e(tmpdir):
    _index_and_repackage(tmpdir, [ATHENA, '--crawl', CRAWL, '--hostname', HOST])


@requires_live_aws
@requires_duckdb
def test_index_duckdb_e2e(tmpdir):
    _index_and_repackage(tmpdir, ['cc-url-index-s3', '--crawl', CRAWL, '--hostname', HOST])


@requires_live_aws
@requires_duckdb
def test_index_duckdb_domain_e2e(tmpdir):
    _index_and_repackage(tmpdir, ['cc-url-index-s3', '--crawl', CRAWL, '--domain', HOST, '--limit', '20'])


@requires_live_aws
@requires_duckdb
def test_index_duckdb_sql_e2e(tmpdir):
    sql = os.path.join(str(tmpdir), 'query.sql')
    with open(sql, 'w') as f:
        f.write(f"""
            SELECT url, warc_filename, warc_record_offset, warc_record_length, content_languages
            FROM read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl={CRAWL}/subset=warc/*.parquet')
            WHERE url_host_name = '{HOST}'
            LIMIT 10
        """)
    _index_and_repackage(tmpdir, ['cc-url-index-s3', '--sql', sql, '--yes'])
