import datetime
import json
from unittest.mock import patch

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.indexes import IndexQuery, UsageError, resolve_index
from cdx_toolkit.indexes.backends.cdx_api import CdxApiBackend
from cdx_toolkit.indexes.backends.cdx_files import CdxFilesBackend
from cdx_toolkit.indexes.backends.athena import AthenaBackend, AthenaLocation
from cdx_toolkit.indexes.backends.duckdb import DuckDbBackend
from cdx_toolkit.indexes.backends.sql import normalize_row
from cdx_toolkit.indexes.backends.sql_builder import check_language, query_conditions

from tests.conftest import requires_duckdb


ATHENA = 'athena://athena.us-east-1.amazonaws.com:443/ccindex?S3OutputLocation=s3://my-bucket/athena/'


@pytest.mark.parametrize(
    'location, index_format, backend, backend_location',
    [
        ('cc', 'auto', CdxApiBackend, None),
        ('cc-url-index-s3', 'auto', DuckDbBackend, 's3://commoncrawl/cc-index/table/cc-main/warc/'),
        ('cc-url-index-hf', 'auto', DuckDbBackend, 'hf://buckets/commoncrawl/commoncrawl/cc-index/table/cc-main/warc/'),
        ('./index/part-0.parquet', 'auto', DuckDbBackend, './index/part-0.parquet'),
        ('s3://bucket/index/*.parquet', 'auto', DuckDbBackend, 's3://bucket/index/*.parquet'),
        ('s3://bucket/index/', 'duckdb', DuckDbBackend, 's3://bucket/index/'),
        (ATHENA, 'auto', AthenaBackend, ATHENA),
        ('s3://bucket/indexes/cdx-00000.gz', 'auto', CdxFilesBackend, 's3://bucket/indexes/cdx-00000.gz'),
        ('./indexes/*.cdx', 'cdx', CdxFilesBackend, './indexes/*.cdx'),
    ],
)
def test_resolve_index(location, index_format, backend, backend_location):
    resolved = resolve_index(location, index_format)
    assert isinstance(resolved, backend)
    if backend_location:
        assert resolved.location == backend_location


@pytest.mark.parametrize(
    'location, index_format, error',
    [
        ('cc', 'duckdb', 'cannot be queried with DuckDB'),
        ('s3://bucket/index/', 'athena', 'invalid Athena location'),
        ('athena://athena.us-east-1.amazonaws.com/ccindex', 'auto', 'S3OutputLocation'),
    ],
)
def test_resolve_index_errors(location, index_format, error):
    with pytest.raises(UsageError, match=error):
        resolve_index(location, index_format)


def test_athena_location():
    assert AthenaLocation.parse(ATHENA) == AthenaLocation('us-east-1', 'ccindex', 'ccindex', 's3://my-bucket/athena/')
    assert AthenaLocation.parse(
        'athena://athena.eu-west-1.amazonaws.com/mydb?S3OutputLocation=s3://b/p/&table=urls'
    ) == AthenaLocation('eu-west-1', 'mydb', 'urls', 's3://b/p/')


def test_normalize_row():
    row = {
        'url': 'https://example.com/',
        'fetch_time': datetime.datetime(2024, 7, 13, 10, 22, 33),
        'fetch_status': 200,
        'warc_record_offset': 10,
        'content_languages': None,
        'my_column': 'x',
    }
    assert normalize_row(row) == {
        'url': 'https://example.com/', 'timestamp': '20240713102233', 'status': '200', 'offset': '10',
        'languages': None, 'my_column': 'x',
    }
    # Athena returns strings
    assert normalize_row({'fetch_time': '2024-07-13 10:22:33.000'}) == {'timestamp': '20240713102233'}


@pytest.mark.parametrize(
    'query, error',
    [
        (IndexQuery(urls=['ex*ample.com']), 'invalid URL pattern'),
        (IndexQuery(sql='SELECT 1', urls=['example.com/*']), '--url cannot be combined with --sql'),
        (IndexQuery(domains=['example.com'], filters=['=status:200']), '--filter cannot be used'),
        (IndexQuery(url_file='urls.csv'), 'use a custom --sql query that joins the index with a table'),
        (IndexQuery(domains=['example.com'], languages=['german']), 'invalid language'),
        (IndexQuery(domains=['example.com'], languages=['en']), 'use ISO 639-3 codes'),
        (IndexQuery(domains=['example.com'], to='2024-01'), 'invalid timestamp'),
        (IndexQuery(sql='SELECT 1', from_ts='2024'), '--from cannot be combined with --sql'),
        (IndexQuery(), 'needs --url, --hostname, --domain or --sql'),
        (
            IndexQuery(sql='SELECT 1', domains=['example.com'], limit=1),
            '--domain, --limit cannot be combined with --sql',
        ),
    ],
)
def test_sql_validate(query, error):
    with pytest.raises(UsageError, match=error):
        DuckDbBackend('s3://bucket/index/').validate(query)


def test_duckdb_guided_query_prunes_crawls():
    backend = DuckDbBackend('s3://bucket/index/')
    query = IndexQuery(hostnames=['www.example.com'], domains=['example.org'], crawl='CC-MAIN-2024-33', limit=5)

    with patch('cdx_toolkit.indexes.backends.sql.resolve_crawl_names', return_value=['CC-MAIN-2024-33']):
        plan = backend.plan(query)
    sql = plan.sql

    assert plan.n_crawls == 1
    assert "read_parquet(['s3://bucket/index/crawl=CC-MAIN-2024-33/subset=warc/*.parquet']" in sql
    assert 'crawl IN' not in sql  # pruned by the glob
    assert ("url_host_name = 'www.example.com' OR (url_host_registered_domain IN ('example.org') "
            "AND (url_surtkey LIKE 'org,example)%' ESCAPE '\\' "
            "OR url_surtkey LIKE 'org,example,%' ESCAPE '\\'))") in sql
    assert 'url_surtkey, fetch_time, url,' in sql
    assert sql.rstrip().endswith('LIMIT 5')


def test_guided_query_url_patterns():
    query = IndexQuery(urls=['example.com/a_b*', 'https://www.example.org/', '*.example.net'], domains=['example.com'])
    sql = AthenaBackend(ATHENA).plan(query).sql

    assert "(url_host_tld = 'com' OR url_host_tld = 'net' OR url_host_tld = 'org')" in sql
    assert ("((url_host_registered_domain IN ('example.com') AND (url_surtkey LIKE 'com,example)%' ESCAPE '\\' "
            "OR url_surtkey LIKE 'com,example,%' ESCAPE '\\')) OR url_surtkey = 'org,example)/' "
            "OR url_surtkey LIKE 'com,example)/a\\_b%' ESCAPE '\\' "
            "OR url_surtkey LIKE 'net,example)%' ESCAPE '\\' OR url_surtkey LIKE 'net,example,%' ESCAPE '\\')") in sql


def test_guided_query_time_range_and_languages():
    query = IndexQuery(hostnames=['example.com'], from_ts='2024', to='20240410', languages=['deu', 'fra'])
    sql = AthenaBackend(ATHENA).plan(query).sql

    assert "fetch_time >= TIMESTAMP '2024-01-01 00:00:00'" in sql
    assert "fetch_time < TIMESTAMP '2024-04-11 00:00:00'" in sql
    assert ("(content_languages = 'deu' OR content_languages LIKE 'deu,%' "
            "OR content_languages = 'fra' OR content_languages LIKE 'fra,%')") in sql


def test_athena_guided_query_and_cost_guard():
    backend = AthenaBackend(ATHENA)
    query = IndexQuery(domains=['example.com'])

    plan = backend.plan(query)
    sql = plan.sql
    assert plan.n_crawls is None
    assert 'FROM ccindex' in sql

    # not restricted to crawls -> confirmation needed, refused in non-interactive mode
    with patch('cdx_toolkit.indexes.backends.sql.sys.stdin') as stdin, patch.object(backend, 'iter_sql_rows') as rows:
        stdin.isatty.return_value = False
        with pytest.raises(SystemExit, match='--yes'):
            list(backend.records(query))
        rows.assert_not_called()

    backend.assume_yes = True
    with patch.object(backend, 'iter_sql_rows', return_value=iter([{'url': 'https://example.com/'}])) as rows:
        assert list(backend.records(query)) == [{'url': 'https://example.com/'}]
        assert rows.call_args.args == (sql,)


def test_sql_count_and_explain():
    backend = AthenaBackend(ATHENA, assume_yes=True)
    query = IndexQuery(sql='SELECT url FROM ccindex WHERE crawl = \'CC-MAIN-2024-33\';\n')

    with patch.object(backend, 'iter_sql_rows', return_value=iter([{'n': '42'}])) as rows:
        assert backend.count(query) == 42
    assert rows.call_args.args == (
        "SELECT count(*) AS n FROM (SELECT url FROM ccindex WHERE crawl = 'CC-MAIN-2024-33') AS q",
    )

    assert 'SELECT url FROM ccindex' in backend.explain(query)


@pytest.fixture
def parquet_index(tmp_path):
    """A tiny columnar index, partitioned like Common Crawl's (crawl=.../subset=warc/*.parquet)."""
    import duckdb

    base = tmp_path / 'index'
    rows = [
        ('CC-MAIN-2024-33', 'org,example)/', 'https://example.org/', 'example.org', 'www.example.org', 'a.warc.gz', 30),
        ('CC-MAIN-2024-33', 'com,example)/', 'https://example.com/', 'example.com', 'example.com', 'b.warc.gz', 20),
        ('CC-MAIN-2024-33', 'com,example)/x', 'https://example.com/x', 'example.com', 'example.com', 'a.warc.gz', 10),
        ('CC-MAIN-2024-30', 'com,example)/', 'https://example.com/', 'example.com', 'example.com', 'c.warc.gz', 0),
    ]
    con = duckdb.connect()
    for crawl in ('CC-MAIN-2024-30', 'CC-MAIN-2024-33'):
        path = base / f'crawl={crawl}' / 'subset=warc'
        path.mkdir(parents=True)
        values = ', '.join(
            f"('{r[1]}', TIMESTAMP '2024-08-01 12:00:00', '{r[2]}', 'text/html', 200, '{r[3]}', '{r[4]}', "
            f"'{r[3].split('.')[-1]}', '{r[5]}', {r[6]}, 100)"
            for r in rows if r[0] == crawl
        )
        con.execute(f'''
            COPY (SELECT *, NULL::VARCHAR AS content_mime_detected, NULL::VARCHAR AS content_digest,
                         NULL::VARCHAR AS content_languages, NULL::VARCHAR AS content_charset,
                         NULL::VARCHAR AS fetch_redirect
                  FROM (VALUES {values}) AS t(url_surtkey, fetch_time, url, content_mime_type,
                  fetch_status, url_host_registered_domain, url_host_name, url_host_tld, warc_filename,
                  warc_record_offset, warc_record_length))
            TO '{path}/part-0.parquet' (FORMAT parquet)
        ''')
    return base


@requires_duckdb
def test_index_duckdb_end_to_end(parquet_index, tmp_path, capsys):
    output = tmp_path / 'out.jsonl'
    with patch('cdx_toolkit.indexes.backends.sql.resolve_crawl_names', return_value=['CC-MAIN-2024-33']):
        main([
            'index', str(parquet_index), '--index-format', 'duckdb', '--domain', 'example.com',
            '--crawl', 'CC-MAIN-2024-33', '-o', str(output),
        ])

    records = [json.loads(line) for line in output.read_text().splitlines()]
    # sorted by WARC file and offset
    assert [(r['filename'], r['offset']) for r in records] == [('a.warc.gz', '10'), ('b.warc.gz', '20')]
    assert records[0] == {
        'urlkey': 'com,example)/x', 'timestamp': '20240801120000', 'url': 'https://example.com/x', 'mime': 'text/html',
        'status': '200', 'filename': 'a.warc.gz', 'offset': '10', 'length': '100', 'crawl': 'CC-MAIN-2024-33',
    }


@requires_duckdb
def test_index_duckdb_parquet_glob_sql_and_count(parquet_index, tmp_path, capsys, monkeypatch):
    files = f'{parquet_index}/*/*/*.parquet'

    # all crawls -> needs --yes in non-interactive mode
    main(['index', files, '--hostname', 'www.example.org', '--yes', '--output-format', 'csv', '--fields', 'url,crawl'])
    assert capsys.readouterr().out.splitlines() == ['url,crawl', 'https://example.org/,CC-MAIN-2024-33']

    main(['index', files, '--domain', 'example.com', '--count', '--yes'])
    assert capsys.readouterr().out == '3\n'

    sql = tmp_path / 'query.sql'
    sql.write_text(f"SELECT url, warc_filename FROM read_parquet('{files}') WHERE warc_record_offset > 15;")
    main(['index', files, '--sql', str(sql), '--yes', '--output-format', 'csv'])
    assert sorted(capsys.readouterr().out.splitlines()) == [
        'https://example.com/,b.warc.gz', 'https://example.org/,a.warc.gz', 'url,filename'
    ]

    main(['index', files, '--sql', str(sql), '--dry-run'])
    assert 'WHERE warc_record_offset > 15' in capsys.readouterr().out


@requires_duckdb
def test_index_duckdb_time_range_and_language(tmp_path, capsys):
    import duckdb

    path = tmp_path / 'crawl=CC-MAIN-2024-33' / 'subset=warc'
    path.mkdir(parents=True)
    rows = [
        # url, fetch_time (UTC), content_languages
        ('https://example.com/a', '2024-08-01 23:30:00', 'deu'),
        ('https://example.com/b', '2024-08-02 00:30:00', 'eng,deu'),
        ('https://example.com/c', '2024-08-03 12:00:00', 'deu,eng'),
        ('https://example.com/d', '2024-08-04 12:00:00', None),
    ]
    values = ', '.join(
        f"('{url}', 'com,example)/{url[-1]}', TIMESTAMPTZ '{t}+00', {repr(lang) if lang else 'NULL'}, 'example.com', "
        f"'example.com', 'com', 'a.warc.gz', {i}, 100)"
        for i, (url, t, lang) in enumerate(rows)
    )
    duckdb.connect().execute(f'''
        COPY (SELECT *, NULL::VARCHAR AS content_mime_type,
                     NULL::VARCHAR AS content_mime_detected, NULL::SMALLINT AS fetch_status,
                     NULL::VARCHAR AS content_digest, NULL::VARCHAR AS content_charset, NULL::VARCHAR AS fetch_redirect
              FROM (VALUES {values}) AS t(url, url_surtkey, fetch_time, content_languages, url_host_registered_domain,
                    url_host_name, url_host_tld, warc_filename, warc_record_offset, warc_record_length))
        TO '{path}/part-0.parquet' (FORMAT parquet)
    ''')
    files = f'{tmp_path}/*/*/*.parquet'

    def urls(*args):
        main(['index', files, '--domain', 'example.com', '--yes', '--output-format', 'csv', '--fields', 'url,timestamp']
             + list(args))
        return capsys.readouterr().out.splitlines()[1:]

    # timestamps are UTC, the day of --to is included
    assert urls('--from', '20240802', '--to', '20240803') == [
        'https://example.com/b,20240802003000', 'https://example.com/c,20240803120000',
    ]
    # the primary (first) content language
    assert urls('--language', 'deu') == ['https://example.com/a,20240801233000', 'https://example.com/c,20240803120000']
    assert urls('--language', 'eng', '--language', 'deu', '--to', '2024080123') == [
        'https://example.com/a,20240801233000',
    ]


@pytest.mark.parametrize('code, language', [('eng', 'eng'), ('DEU', 'deu'), (' fra ', 'fra'), ('nds', 'nds')])
def test_check_language(code, language):
    assert check_language(code) == language


def test_languages_are_normalized_in_the_query():
    sql = AthenaBackend(ATHENA).plan(IndexQuery(hostnames=['example.com'], languages=['DEU'])).sql
    assert "content_languages = 'deu'" in sql


@requires_duckdb
def test_index_duckdb_url_patterns(parquet_index, capsys):
    files = f'{parquet_index}/*/*/*.parquet'

    def urls(*args):
        main(['index', files, '--yes', '--output-format', 'csv', '--fields', 'url'] + list(args))
        return sorted(capsys.readouterr().out.splitlines()[1:])

    assert urls('--url', 'example.com/x') == ['https://example.com/x']
    assert urls('--url', 'example.com/*') == ['https://example.com/', 'https://example.com/', 'https://example.com/x']
    assert urls('--url', '*.example.org', '--url', 'example.com/x') == ['https://example.com/x', 'https://example.org/']


@requires_duckdb
def test_sql_without_url_column_to_repackage(parquet_index, tmp_path, capsys):
    """A --sql query that selects only the WARC location is enough for repackaging."""
    from tests.fake_warc_server import FakeWarcServer

    server = FakeWarcServer('https://data.example.org')
    capture, = server.add_captures('a.warc.gz', ['https://example.com/x'])
    files = f'{parquet_index}/*/*/*.parquet'
    sql = tmp_path / 'query.sql'
    sql.write_text(
        f"SELECT warc_filename, {capture['offset']} AS warc_record_offset, {capture['length']} AS warc_record_length "
        f"FROM read_parquet('{files}') WHERE url = 'https://example.com/x' AND crawl = 'CC-MAIN-2024-33'"
    )
    captures = tmp_path / 'captures.jsonl'
    main(['index', files, '--sql', str(sql), '--yes', '-o', str(captures)])
    assert [json.loads(line) for line in captures.read_text().splitlines()] == [
        {'filename': 'a.warc.gz', 'offset': capture['offset'], 'length': capture['length']}
    ]
    with patch('cdx_toolkit.repackage.readers.http.myrequests_get', side_effect=server.get):
        main(['repackage', str(captures), '--prefix', str(tmp_path / 'out'),
              '--warc-download-prefix', 'https://data.example.org'])
    assert len(list(tmp_path.glob('out-*.warc.gz'))) == 1

    with pytest.raises(SystemExit) as exc:
        main(['index', files, '--sql', str(sql), '--yes', '--url-fgrep', 'example'])
    assert exc.value.code == 2
    assert '--url-fgrep and --url-fgrepv need captures with a url' in capsys.readouterr().err


@requires_duckdb
@pytest.mark.parametrize('to, included', [
    ('20240801', ['23:59:59.500']),
    ('20240801235959', ['23:59:59.500']),
    ('2024080123595', ['23:59:59.500']),
    ('20240802', ['23:59:59.500', 'next day']),
])
def test_end_timestamp_includes_fractional_seconds(to, included):
    import duckdb

    rows = {'23:59:59.500': '2024-08-01 23:59:59.500', 'next day': '2024-08-02 00:00:00'}
    con = duckdb.connect()
    con.execute('CREATE TABLE t (name VARCHAR, fetch_time TIMESTAMP)')
    con.executemany('INSERT INTO t VALUES (?, ?)', list(rows.items()))
    condition, = query_conditions(IndexQuery(to=to))
    assert sorted(r[0] for r in con.execute(f'SELECT name FROM t WHERE {condition}').fetchall()) == included


def test_end_timestamp_overflow():
    with pytest.raises(UsageError, match='invalid timestamp'):
        query_conditions(IndexQuery(to='9999'))
