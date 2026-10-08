"""Gated end-to-end tests for the SQL indexes (Athena, DuckDB): `cdxt index ... | cdxt repackage`.

These query only a single crawl partition for a single host (cheap, partition-pruned
-- never an all-crawls scan), write the captures with `cdxt index`, then fetch the
WARC records over HTTP with `cdxt repackage`. They require AWS credentials (and duckdb)
and are skipped in CI.
"""
import json
import os

import fsspec
import pytest
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.cli import main
from tests.conftest import requires_aws_athena, requires_aws_s3, requires_duckdb, TEST_ATHENA_S3_LOCATION


CRAWL = 'CC-MAIN-2026-17'
HOST = 'commoncrawl.org'
ATHENA = f'athena://athena.us-east-1.amazonaws.com/ccindex?S3OutputLocation={TEST_ATHENA_S3_LOCATION}'


@pytest.fixture(autouse=True)
def live_crawl_list_cache(tmp_path, monkeypatch):
    """--crawl is resolved with the live list of crawls; cache it apart from the (mocked) list of other tests."""
    monkeypatch.setattr('cdx_toolkit.commoncrawl.CACHE_DIR', str(tmp_path / 'cache') + '/')


def _index_and_repackage(tmpdir, index_args):
    captures_path = os.path.join(str(tmpdir), 'captures.jsonl')
    main(args=['index'] + index_args + ['-o', captures_path])

    with open(captures_path) as f:
        captures = [json.loads(line) for line in f]
    assert len(captures) > 0, 'expected at least one capture for the host/crawl'
    assert all(HOST in c['url'] for c in captures)

    base_prefix = str(tmpdir)
    main(args=['repackage', captures_path, f'--prefix={base_prefix}/TEST_sql'])

    response_count = 0
    with fsspec.open(os.path.join(base_prefix, 'TEST_sql-001.warc.gz'), 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                response_count += 1
                target = record.rec_headers.get_header('WARC-Target-URI') or ''
                assert HOST in target, f'unexpected target URI: {target}'

    assert response_count == len(captures), 'every capture should yield a response record'


@requires_aws_athena
def test_index_athena_e2e(tmpdir):
    _index_and_repackage(tmpdir, [ATHENA, '--crawl', CRAWL, '--hostname', HOST])


@requires_aws_s3
@requires_duckdb
def test_index_duckdb_e2e(tmpdir):
    _index_and_repackage(tmpdir, ['cc-url-index-s3', '--crawl', CRAWL, '--hostname', HOST])


@requires_aws_s3
@requires_duckdb
def test_index_duckdb_domain_e2e(tmpdir):
    _index_and_repackage(tmpdir, ['cc-url-index-s3', '--crawl', CRAWL, '--domain', HOST, '--limit', '20'])


@requires_aws_s3
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
