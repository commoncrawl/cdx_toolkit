"""Building the guided SQL queries for the columnar index (no engine)."""
import pytest

from cdx_toolkit.indexes import IndexQuery
from cdx_toolkit.indexes.backends.sql_builder import build_sql, escape_sql_literal


def test_build_query_hostnames():
    q = build_sql(IndexQuery(hostnames=['example.com', 'test.org']), 'ccindex')
    assert "subset = 'warc'" in q
    assert "url_host_tld = 'com'" in q
    assert "url_host_tld = 'org'" in q
    assert "url_host_name = 'example.com'" in q
    assert "url_host_name = 'test.org'" in q
    assert 'warc_filename, warc_record_offset, warc_record_length' in q
    assert 'crawl IN' not in q
    assert 'LIMIT' not in q


def test_build_query_domains_only():
    q = build_sql(IndexQuery(domains=['example.com']), 'ccindex')
    assert "url_host_registered_domain IN ('example.com')" in q
    assert "url_host_tld = 'com'" in q
    assert 'url_host_name' not in q


def test_build_query_hostnames_and_domains():
    q = build_sql(IndexQuery(hostnames=['www.example.com'], domains=['example.org']), 'ccindex')
    assert "url_host_name = 'www.example.com'" in q
    assert "url_host_registered_domain IN ('example.org')" in q
    assert "url_host_tld = 'com'" in q
    assert "url_host_tld = 'org'" in q


def test_build_query_requires_host_or_domain():
    with pytest.raises(ValueError):
        build_sql(IndexQuery(), 'ccindex')


def test_build_query_with_crawls():
    q = build_sql(IndexQuery(hostnames=['example.com']), 'ccindex', crawls=['CC-MAIN-2025-33', 'CC-MAIN-2025-30'])
    assert "crawl IN ('CC-MAIN-2025-33', 'CC-MAIN-2025-30')" in q


def test_build_query_limit():
    assert 'LIMIT 10' in build_sql(IndexQuery(hostnames=['example.com'], limit=10), 'ccindex')
    assert 'LIMIT' not in build_sql(IndexQuery(hostnames=['example.com']), 'ccindex')


def test_build_query_orders_by_default():
    q = build_sql(IndexQuery(hostnames=['example.com']), 'ccindex')
    assert 'ORDER BY warc_filename, warc_record_offset' in q


def test_build_query_order_by_false():
    assert 'ORDER BY' not in build_sql(IndexQuery(hostnames=['example.com']), 'ccindex', order_by=False)


def test_build_query_order_by_precedes_limit():
    q = build_sql(IndexQuery(hostnames=['example.com'], limit=10), 'ccindex')
    assert q.index('ORDER BY') < q.index('LIMIT')


def test_escape_sql_literal_valid():
    assert escape_sql_literal('example.com') == "'example.com'"
    assert escape_sql_literal('CC-MAIN-2025-33') == "'CC-MAIN-2025-33'"


@pytest.mark.parametrize('bad', ["a';DROP TABLE x;--", "a' OR '1'='1", 'a b', "a'", "', '"])
def test_escape_sql_literal_rejects_injection(bad):
    with pytest.raises(ValueError):
        escape_sql_literal(bad)


def test_build_query_rejects_injection_hostname():
    with pytest.raises(ValueError):
        build_sql(IndexQuery(hostnames=["example.com'; DROP TABLE ccindex; --"]), 'ccindex')


def test_resolve_crawl_names():
    from unittest.mock import patch

    from cdx_toolkit.indexes.backends.sql_builder import resolve_crawl_names

    endpoints = [f'https://index.commoncrawl.org/CC-MAIN-2024-{week}-index' for week in (30, 33, 38)]
    with patch('cdx_toolkit.indexes.backends.sql_builder.get_cc_endpoints', return_value=endpoints):
        assert resolve_crawl_names('2') == ['CC-MAIN-2024-33', 'CC-MAIN-2024-38']
        assert resolve_crawl_names('CC-MAIN-2024-30') == ['CC-MAIN-2024-30']
