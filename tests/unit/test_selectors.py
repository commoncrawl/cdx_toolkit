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
        COPY (SELECT *, array_to_string(list_reverse(string_split(url_host_name, '.')), '.') AS url_host_name_reversed,
                     TIMESTAMP '2024-08-01 12:00:00' AS fetch_time, 'a.warc.gz' AS warc_filename,
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
    (['--domain', 'Example.COM.'], [
        'https://a.blog.example.com/', 'https://ablog.example.com/', 'https://blog.example.com/',
        'https://example.com/', 'https://www.example.com/w',
    ]),
    (['--hostname', 'blog.example.com'], ['https://blog.example.com/']),
    # exact hosts, also with www. (which SURT keys drop)
    (['--hostname', 'example.com'], ['https://example.com/']),
    (['--hostname', 'www.example.com'], ['https://www.example.com/w']),
    (['--hostname', 'WWW.Example.com'], ['https://www.example.com/w']),
    (['--hostname', 'www.example.com', '--url', 'blog.example.com/*'],
     ['https://blog.example.com/', 'https://www.example.com/w']),
    (['--hostname', 'www.example.com', '--domain', 'example.com.au'],
     ['https://example.com.au/', 'https://www.example.com/w']),
    (['--url', '*.blog.example.com'], ['https://a.blog.example.com/', 'https://blog.example.com/']),
    (['--url', 'blog.example.com/*'], ['https://blog.example.com/']),
])
def test_selectors_match_the_same_captures_on_every_backend(indexes, capsys, selector, expected):
    for index in indexes:
        main(['index', index, '--yes', '--output-format', 'csv', '--fields', 'url'] + selector)
        assert sorted(capsys.readouterr().out.splitlines()[1:]) == expected, index


@requires_duckdb
def test_count_is_capped_by_the_limit_on_every_backend(indexes, capsys):
    for index in indexes:
        for limit, expected in ((None, '5'), ('2', '2'), ('9', '5')):
            limit_args = ['--limit', limit] if limit else []
            main(['index', index, '--yes', '--domain', 'example.com', '--count'] + limit_args)
            assert capsys.readouterr().out.strip() == expected, (index, limit)
