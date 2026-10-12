"""The URL selectors select the same captures from every backend (CDX files, the columnar index with DuckDB)."""
import json

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.indexes.patterns import to_surt
from tests.conftest import requires_duckdb


# url, registered domain
CAPTURES = [
    ('https://example.com/', 'example.com'),
    ('https://www.example.com/w', 'example.com'),
    ('https://blog.example.com/', 'example.com'),
    ('https://a.blog.example.com/', 'example.com'),
    ('https://ablog.example.com/', 'example.com'),
    ('https://blogexample.com/', 'blogexample.com'),
    ('https://example.com.au/', 'example.com.au'),
]


@pytest.fixture(scope='module')
def indexes(tmp_path_factory):
    """The same captures as a CDX file and as a columnar (parquet) index."""
    import duckdb

    base = tmp_path_factory.mktemp('indexes')
    lines = sorted(
        f'{to_surt(url)} 20240801120000 '
        f'{json.dumps({"url": url, "filename": "a.warc.gz", "offset": str(i), "length": "1"})}\n'
        for i, (url, _) in enumerate(CAPTURES)
    )
    (base / 'index.cdx').write_text(''.join(lines))

    path = base / 'columnar' / 'crawl=CC-MAIN-2024-33' / 'subset=warc'
    path.mkdir(parents=True)
    values = ', '.join(
        f"('{to_surt(url)}', '{url}', '{url.split('/')[2]}', '{domain}', '{domain.rsplit('.', 1)[-1]}', {i})"
        for i, (url, domain) in enumerate(CAPTURES)
    )
    duckdb.connect().execute(f'''
        COPY (SELECT *, TIMESTAMP '2024-08-01 12:00:00' AS fetch_time, 'a.warc.gz' AS warc_filename,
                     1 AS warc_record_length, NULL::VARCHAR AS content_mime_type,
                     NULL::VARCHAR AS content_mime_detected, NULL::SMALLINT AS fetch_status,
                     NULL::VARCHAR AS content_digest, NULL::VARCHAR AS content_languages,
                     NULL::VARCHAR AS content_charset, NULL::VARCHAR AS fetch_redirect
              FROM (VALUES {values}) AS t(url_surtkey, url, url_host_name, url_host_registered_domain, url_host_tld,
                    warc_record_offset))
        TO '{path}/part-0.parquet' (FORMAT parquet)
    ''')
    return str(base / 'index.cdx'), str(base / 'columnar' / '*' / '*' / '*.parquet')


@requires_duckdb
@pytest.mark.parametrize('selector, expected', [
    (['--domain', 'example.com'], [
        'https://a.blog.example.com/', 'https://ablog.example.com/', 'https://blog.example.com/',
        'https://example.com/', 'https://www.example.com/w',
    ]),
    (['--domain', 'blog.example.com'], ['https://a.blog.example.com/', 'https://blog.example.com/']),
    (['--domain', 'a.blog.example.com'], ['https://a.blog.example.com/']),
    (['--domain', 'example.com.au'], ['https://example.com.au/']),
    (['--hostname', 'blog.example.com'], ['https://blog.example.com/']),
    (['--url', '*.blog.example.com'], ['https://a.blog.example.com/', 'https://blog.example.com/']),
    (['--url', 'blog.example.com/*'], ['https://blog.example.com/']),
])
def test_selectors_match_the_same_captures_on_every_backend(indexes, capsys, selector, expected):
    for index in indexes:
        main(['index', index, '--yes', '--output-format', 'csv', '--fields', 'url'] + selector)
        assert sorted(capsys.readouterr().out.splitlines()[1:]) == expected, index
