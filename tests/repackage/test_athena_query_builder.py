import pytest

from cdx_toolkit.repackage.sources.sql_base import (
    build_athena_query,
    escape_sql_literal,
    validate_result_columns,
    join_warc_url,
)
from cdx_toolkit.repackage.sources.athena import run_athena_query, iter_range_jobs


class _FakeAthenaClient:
    """Minimal stand-in for a boto3 Athena client (no AWS, no network)."""

    def __init__(self, raise_on_wait=None):
        self._raise_on_wait = raise_on_wait
        self.started = []
        self.stopped = []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {'QueryExecutionId': 'qid-123'}

    def get_query_execution(self, QueryExecutionId):
        if self._raise_on_wait is not None:
            raise self._raise_on_wait
        return {'QueryExecution': {'Status': {'State': 'SUCCEEDED'},
                                   'Statistics': {'DataScannedInBytes': 1000}}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)


def test_build_query_hostnames():
    q = build_athena_query(['example.com', 'test.org'])
    assert "subset = 'warc'" in q
    assert "url_host_tld = 'com'" in q
    assert "url_host_tld = 'org'" in q
    assert "url_host_name = 'example.com'" in q
    assert "url_host_name = 'test.org'" in q
    assert 'warc_filename, warc_record_offset, warc_record_length' in q
    assert 'crawl IN' not in q
    assert 'LIMIT' not in q


def test_build_query_domains_only():
    q = build_athena_query(url_host_registered_domains=['example.com'])
    assert "url_host_registered_domain = 'example.com'" in q
    assert "url_host_tld = 'com'" in q
    assert 'url_host_name' not in q


def test_build_query_hostnames_and_domains():
    q = build_athena_query(['www.example.com'], url_host_registered_domains=['example.org'])
    assert "url_host_name = 'www.example.com'" in q
    assert "url_host_registered_domain = 'example.org'" in q
    assert "url_host_tld = 'com'" in q
    assert "url_host_tld = 'org'" in q


def test_build_query_requires_host_or_domain():
    with pytest.raises(ValueError):
        build_athena_query()


def test_build_query_with_crawls():
    q = build_athena_query(['example.com'], crawls=['CC-MAIN-2025-33', 'CC-MAIN-2025-30'])
    assert "crawl IN ('CC-MAIN-2025-33', 'CC-MAIN-2025-30')" in q


def test_build_query_limit():
    assert 'LIMIT 10' in build_athena_query(['example.com'], limit=10)
    assert 'LIMIT' not in build_athena_query(['example.com'], limit=0)


def test_build_query_orders_by_default():
    q = build_athena_query(['example.com'])
    assert 'ORDER BY warc_filename, warc_record_offset' in q


def test_build_query_order_by_false():
    assert 'ORDER BY' not in build_athena_query(['example.com'], order_by=False)


def test_build_query_order_by_precedes_limit():
    q = build_athena_query(['example.com'], limit=10)
    assert q.index('ORDER BY') < q.index('LIMIT')


def test_build_query_requires_hostnames():
    with pytest.raises(ValueError):
        build_athena_query([])


def test_escape_sql_literal_valid():
    assert escape_sql_literal('example.com') == "'example.com'"
    assert escape_sql_literal('CC-MAIN-2025-33') == "'CC-MAIN-2025-33'"


@pytest.mark.parametrize('bad', ["a';DROP TABLE x;--", "a' OR '1'='1", 'a b', "a'", "', '"])
def test_escape_sql_literal_rejects_injection(bad):
    with pytest.raises(ValueError):
        escape_sql_literal(bad)


def test_build_query_rejects_injection_hostname():
    with pytest.raises(ValueError):
        build_athena_query(["example.com'; DROP TABLE ccindex; --"])


def test_validate_result_columns_ok():
    validate_result_columns(['warc_filename', 'warc_record_offset', 'warc_record_length', 'extra'])


def test_validate_result_columns_missing():
    with pytest.raises(ValueError) as e:
        validate_result_columns(['warc_filename'])
    assert 'warc_record_offset' in str(e.value)
    assert 'warc_record_length' in str(e.value)


def test_join_warc_url_relative():
    assert (
        join_warc_url('https://data.commoncrawl.org', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )


def test_join_warc_url_no_double_slash():
    assert (
        join_warc_url('https://data.commoncrawl.org/', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )
    assert join_warc_url('s3://bucket', '/crawl-data/x.warc.gz') == 's3://bucket/crawl-data/x.warc.gz'


def test_join_warc_url_empty_prefix():
    assert join_warc_url('', 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'
    assert join_warc_url(None, 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'


def test_join_warc_url_absolute_filename_passthrough():
    assert join_warc_url('https://data.commoncrawl.org', 's3://cc/x.warc.gz') == 's3://cc/x.warc.gz'
    assert join_warc_url('', 'https://host/x.warc.gz') == 'https://host/x.warc.gz'


class _FakePaginator:
    """Yields Athena get_query_results-style pages (first row is the header)."""

    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return iter(self._pages)


class _FakeResultsClient:
    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, name):
        assert name == 'get_query_results'
        return _FakePaginator(self._pages)


def _athena_page(columns, *value_rows):
    header = {'Data': [{'VarCharValue': c} for c in columns]}
    data = [{'Data': [{'VarCharValue': v} for v in row]} for row in value_rows]
    return {'ResultSet': {'Rows': [header] + data}}


def test_iter_range_jobs_carries_extra_columns():
    pages = [_athena_page(
        ['warc_filename', 'warc_record_offset', 'warc_record_length', 'content_languages', 'url'],
        ['crawl-data/x.warc.gz', '100', '200', 'eng', 'https://example.com/page'],
    )]
    client = _FakeResultsClient(pages)
    jobs = list(iter_range_jobs(client, 'qid', 'https://data.commoncrawl.org'))
    assert len(jobs) == 1
    job = jobs[0]
    assert job.url == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    assert job.offset == 100 and job.length == 200
    assert job.filename == 'crawl-data/x.warc.gz'
    # extra carries only the non-required columns (page URL distinct from warc URL)
    assert job.extra == {'content_languages': 'eng', 'url': 'https://example.com/page'}


def test_iter_range_jobs_no_extra_columns_is_none():
    pages = [_athena_page(
        ['warc_filename', 'warc_record_offset', 'warc_record_length'],
        ['crawl-data/x.warc.gz', '1', '2'],
    )]
    jobs = list(iter_range_jobs(_FakeResultsClient(pages), 'qid', 'https://data.commoncrawl.org'))
    assert jobs[0].extra is None


def test_run_athena_query_logs_sql(caplog):
    import logging
    client = _FakeAthenaClient()
    with caplog.at_level(logging.INFO):
        run_athena_query(client, 'SELECT my_special_column FROM ccindex', 'ccindex', 's3://b/out/')
    assert 'SELECT my_special_column FROM ccindex' in caplog.text


def test_run_athena_query_success_no_cancel():
    client = _FakeAthenaClient()
    qid = run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert qid == 'qid-123'
    assert client.stopped == []


def test_run_athena_query_cancels_on_interrupt():
    client = _FakeAthenaClient(raise_on_wait=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert client.stopped == ['qid-123']


def test_run_athena_query_cancels_on_timeout():
    client = _FakeAthenaClient(raise_on_wait=TimeoutError('too slow'))
    with pytest.raises(TimeoutError):
        run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert client.stopped == ['qid-123']
