from unittest.mock import patch

import pytest

from cdx_toolkit.indexes import IndexQuery, UsageError, resolve_index
from cdx_toolkit.indexes.backends.cdx_api import CdxApiBackend
from cdx_toolkit.indexes.backends.cdx_files import CdxFilesBackend
from cdx_toolkit.indexes.resolve import is_cdx_api_location


@pytest.mark.parametrize(
    'location, is_api',
    [
        ('cc', True),
        ('ia', True),
        ('http://index.commoncrawl.org', True),
        ('https://index.commoncrawl.org/', True),
        ('https://index.commoncrawl.org/CC-MAIN-2024-30-index', True),
        ('https://web.archive.org/cdx/search/cdx', True),
        ('https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2024-30/indexes/cdx-00000.gz', False),
        ('https://example.com/index.cdxj', False),
        ('https://example.com/indexes/*', False),
        ('s3://commoncrawl/cc-index/collections/CC-MAIN-2024-30/indexes/*.cdx.gz', False),
        ('./captures.cdx', False),
        ('/data/index', False),
    ],
)
def test_is_cdx_api_location(location, is_api):
    assert is_cdx_api_location(location) == is_api


def test_resolve_index():
    backend = resolve_index('cc', cc_mirror='https://mirror.example.com/', cc_sort='ascending')
    assert isinstance(backend, CdxApiBackend)
    assert (backend.source, backend.cc_mirror, backend.cc_sort) == ('cc', 'https://mirror.example.com/', 'ascending')

    cdx_files = resolve_index('s3://commoncrawl/cc-index/collections/CC-MAIN-2024-30/indexes/cdx-*.gz')
    assert isinstance(cdx_files, CdxFilesBackend)
    with pytest.raises(UsageError, match='unknown index format'):
        resolve_index('cc', index_format='parquet')


def test_query_options_used():
    query = IndexQuery(urls=['example.com/*'], limit=10)
    assert query.options_used() == {'urls', 'limit'}
    assert IndexQuery().options_used() == set()


def test_cdx_api_validate():
    CdxApiBackend('cc').validate(IndexQuery(domains=['example.com'], crawl='3'))

    with pytest.raises(UsageError, match='at least one --url'):
        CdxApiBackend('cc').validate(IndexQuery(crawl='3'))
    with pytest.raises(UsageError, match='--crawl can only be used'):
        CdxApiBackend('ia').validate(IndexQuery(urls=['example.com'], crawl='3'))


def test_unsupported_option():
    class NoFilterBackend(CdxApiBackend):
        supported_options = CdxApiBackend.supported_options - {'filters'}

    with pytest.raises(UsageError, match='--filter cannot be used with the CDX API index'):
        NoFilterBackend('cc').validate(IndexQuery(urls=['example.com'], filters=['=status:200']))


def _capture(url):
    return {'url': url, 'timestamp': '20240101000000'}


def test_cdx_api_url_patterns_and_limit():
    query = IndexQuery(
        urls=['example.com/*'], hostnames=['www.example.org'], domains=['example.net'],
        from_ts='2020', filters=['=status:200'], limit=3,
    )
    results = {
        'example.com/*': [_capture('http://example.com/a'), _capture('http://example.com/b')],
        'www.example.org': [_capture('http://www.example.org/'), _capture('http://www.example.org/x')],
        'example.net': [_capture('http://example.net/')],
    }

    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        fetcher.return_value.iter.side_effect = lambda url, **params: iter(results[url])
        records = list(CdxApiBackend('cc').records(query))

    assert [r['url'] for r in records] == ['http://example.com/a', 'http://example.com/b', 'http://www.example.org/']

    calls = fetcher.return_value.iter.call_args_list
    assert len(calls) == 2  # the limit was reached before the domain query
    # with several selectors, the limit is applied after removing duplicates, not by the server
    assert calls[0].args == ('example.com/*',)
    assert calls[0].kwargs == {'from_ts': '2020', 'filter': ['=status:200']}
    assert calls[1].args == ('www.example.org',)
    assert calls[1].kwargs == {'from_ts': '2020', 'filter': ['=status:200'], 'matchType': 'host'}


def test_cdx_api_single_selector_limit_is_sent_to_the_server():
    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        fetcher.return_value.iter.return_value = iter([_capture('http://example.com/a')])
        list(CdxApiBackend('cc').records(IndexQuery(urls=['example.com/*'], limit=3)))
    assert fetcher.return_value.iter.call_args.kwargs == {'limit': 3}


def test_cdx_api_overlapping_selectors_return_each_capture_once():
    def capture(url, timestamp, offset):
        return {'url': url, 'timestamp': timestamp, 'filename': 'a.warc.gz', 'offset': offset}

    shared = capture('http://example.com/a', '20240101000000', '1')
    again = capture('http://example.com/a', '20240201000000', '2')  # same URL, another capture
    other_crawl = capture('http://example.com/a', '20240101000000', '3')  # same URL and time, another WARC record
    results = {
        'example.com/*': [shared, again],
        'example.com': [dict(shared), other_crawl, capture('http://www.example.com/', '20240101000000', '4')],
    }
    query = IndexQuery(urls=['example.com/*'], domains=['example.com'])
    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        fetcher.return_value.iter.side_effect = lambda url, **params: iter(results[url])
        records = list(CdxApiBackend('cc').records(query))
        assert [r['offset'] for r in records] == ['1', '2', '3', '4']

        # the limit counts distinct captures
        query.limit = 3
        records = list(CdxApiBackend('cc').records(query))
        assert [r['offset'] for r in records] == ['1', '2', '3']


def test_cdx_api_closest_uses_get_and_crawl_is_normalized():
    query = IndexQuery(domains=['example.com'], closest='20170615', crawl='CC-MAIN-2024-33,CC-MAIN-2024-30')

    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        fetcher.return_value.get.return_value = [_capture('http://example.com/')]
        records = list(CdxApiBackend('cc').records(query))

    assert len(records) == 1
    assert fetcher.call_args.kwargs == {'source': 'cc', 'crawl': ['CC-MAIN-2024-33', 'CC-MAIN-2024-30']}
    assert fetcher.return_value.get.call_args.kwargs == {'closest': '20170615', 'matchType': 'domain'}


def test_cdx_api_count():
    query = IndexQuery(urls=['example.com/*'], domains=['example.org'], limit=10)

    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        fetcher.return_value.get_size_estimate.side_effect = [3000, 1500, 3000, 1500]
        assert CdxApiBackend('ia').count(query) == 10  # capped by the limit
        calls = fetcher.return_value.get_size_estimate.call_args_list
        assert calls[1].args == ('example.org',)
        assert calls[1].kwargs == {'limit': 10, 'matchType': 'domain'}

        query.limit = None
        assert CdxApiBackend('ia').count(query) == 4500


def test_records_url_fgrep():
    backend = CdxApiBackend('cc')
    query = IndexQuery(urls=['example.com/*'], url_fgrep='/keep', url_fgrepv='robots.txt')
    captures = [
        _capture('http://example.com/drop'),
        _capture('http://example.com/keep/robots.txt'),
        _capture('http://example.com/keep/this'),
    ]

    with patch.object(backend, 'select', return_value=iter(captures)):
        records = list(backend.records(query))

    assert [r['url'] for r in records] == ['http://example.com/keep/this']
