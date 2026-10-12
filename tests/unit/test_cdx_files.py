import gzip
import json
import shutil

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.indexes import IndexQuery, UsageError, iter_index_records
from cdx_toolkit.indexes.cdx_files import (
    CdxFilesBackend, domain_entry_to_surts, load_url_patterns, url_pattern_to_surts,
)
from cdx_toolkit.record_io import write_records

from tests.conftest import TEST_DATA_PATH


CDX_PATH = TEST_DATA_PATH / 'repackage' / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'

with gzip.open(CDX_PATH, 'rt') as _f:
    LINES = _f.read().splitlines()


def count_lines(prefix, day=None):
    return sum(1 for line in LINES if line.startswith(prefix) and (day is None or line.split(' ')[1].startswith(day)))


@pytest.mark.parametrize(
    'pattern, prefixes, exact',
    [
        ('example.com/*', ['com,example)/'], []),
        ('example.com/path*', ['com,example)/path'], []),
        ('*.example.com', ['com,example)', 'com,example,'], []),
        ('https://www.example.com/a', [], ['com,example)/a']),
    ],
)
def test_url_pattern_to_surts(pattern, prefixes, exact):
    assert url_pattern_to_surts(pattern) == (prefixes, exact)


@pytest.mark.parametrize('pattern', ['ex*ample.com', '*.example.com/path', '*.'])
def test_invalid_url_patterns(pattern):
    with pytest.raises(UsageError):
        url_pattern_to_surts(pattern)


def test_select_by_hostname_domain_and_url():
    backend = CdxFilesBackend(str(CDX_PATH))

    assert backend.count(IndexQuery(hostnames=['biodiv.mnhn.fr'])) == count_lines('fr,mnhn,biodiv)')
    assert backend.count(IndexQuery(domains=['missiondefrance.fr'])) == count_lines('fr,missiondefrance,')
    assert backend.count(IndexQuery(domains=['mnhn.fr'], urls=['*.missiondefrance.fr'])) == len(LINES)
    assert backend.count(IndexQuery(urls=['biodiv.mnhn.fr/fr/taxonomy*'])) == count_lines('fr,mnhn,biodiv)/fr/taxonomy')

    exact_key = LINES[0].split(' ')[0]
    records = list(backend.iter_records(IndexQuery(urls=[json.loads(LINES[0].split(' ', 2)[2])['url']])))
    assert {r['urlkey'] for r in records} == {exact_key}


def test_time_range_fgrep_and_limit():
    backend = CdxFilesBackend(str(CDX_PATH))
    query = IndexQuery(domains=['missiondefrance.fr'], from_ts='20240716', to='20240716')
    assert backend.count(query) == count_lines('fr,missiondefrance,', day='20240716')

    records = list(iter_index_records(backend, IndexQuery(domains=['mnhn.fr'], url_fgrep='/taxonomy/', limit=5)))
    assert 0 < len(records) <= 5
    assert all('/taxonomy/' in r['url'] and r['urlkey'].startswith('fr,mnhn,biodiv)') for r in records)
    assert set(records[0]) >= {'urlkey', 'timestamp', 'url', 'filename', 'offset', 'length'}


def test_limit_applies_before_the_url_filters(tmp_path, capsys):
    path = tmp_path / 'index.cdx'
    path.write_text(''.join(
        f'com,example)/{name} 20240101000000 {json.dumps({"url": f"https://example.com/{name}"})}\n'
        for name in ('drop', 'keep')
    ))
    backend = CdxFilesBackend(str(path))
    query = IndexQuery(domains=['example.com'], limit=1, url_fgrep='keep')
    assert list(iter_index_records(backend, query)) == []
    query.url_fgrep, query.url_fgrepv = None, 'drop'
    assert list(iter_index_records(backend, query)) == []
    assert backend.count(IndexQuery(domains=['example.com'], limit=1)) == 1
    assert backend.count(IndexQuery(domains=['example.com'])) == 2


def test_url_file_formats(tmp_path):
    urls = ['*.missiondefrance.fr', 'biodiv.mnhn.fr/fr/taxonomy*']
    txt = tmp_path / 'urls.txt'
    txt.write_text('# target URLs\n' + '\n'.join(urls) + '\n\n')
    assert load_url_patterns(str(txt)) == urls

    write_records(({'name': str(i), 'target': u} for i, u in enumerate(urls)), str(tmp_path / 'urls.csv'), 'csv')
    assert load_url_patterns(str(tmp_path / 'urls.csv'), 'target') == urls
    with pytest.raises(UsageError, match="no column 'url'"):
        load_url_patterns(str(tmp_path / 'urls.csv'))
    with pytest.raises(UsageError, match='--url-column needs'):
        load_url_patterns(str(txt), 'target')

    backend = CdxFilesBackend(str(CDX_PATH))
    expected = count_lines('fr,missiondefrance,') + count_lines('fr,mnhn,biodiv)/fr/taxonomy')
    assert backend.count(IndexQuery(url_file=str(txt))) == expected
    assert backend.count(IndexQuery(url_file=str(tmp_path / 'urls.csv'), url_column='target')) == expected


def test_url_file_parquet(tmp_path):
    pytest.importorskip('pyarrow')
    write_records(iter([{'url': '*.missiondefrance.fr'}]), str(tmp_path / 'urls.parquet'), 'parquet')
    assert load_url_patterns(str(tmp_path / 'urls.parquet')) == ['*.missiondefrance.fr']


@pytest.mark.parametrize('processes', [1, 2])
def test_one_output_per_cdx_file(tmp_path, processes):
    for name in ('a/cdx-00001.gz', 'b/cdx-00002.gz', 'b/cdx-00003.gz'):
        (tmp_path / 'in' / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(CDX_PATH, tmp_path / 'in' / name)
    # no match in this file: no output
    with gzip.open(tmp_path / 'in' / 'b' / 'cdx-00003.gz', 'wt') as f:
        f.write(next(line for line in LINES if line.startswith('fr,missiondefrance')) + '\n')
    out = tmp_path / 'out'

    main([
        'index', str(tmp_path / 'in' / '*' / 'cdx-*.gz'), '--hostname', 'biodiv.mnhn.fr', '-o', str(out),
        '--processes', str(processes),
    ])

    outputs = sorted(p.relative_to(out).as_posix() for p in out.rglob('*') if p.is_file())
    assert outputs == ['a/cdx-00001.gz', 'b/cdx-00002.gz']
    with gzip.open(out / 'a' / 'cdx-00001.gz', 'rt') as f:
        # the lines are copied unchanged
        assert f.read().splitlines() == [line for line in LINES if line.startswith('fr,mnhn,biodiv)')]


def test_cli_combined_output_and_errors(tmp_path, capsys):
    main(['index', str(CDX_PATH), '--domain', 'mnhn.fr', '--limit', '3', '--fields', 'url', '--output-format', 'csv'])
    out = capsys.readouterr().out.splitlines()
    assert out[0] == 'url' and len(out) == 4

    for args, error in [
        ([], 'select captures from the CDX files'),
        (['--domain', 'mnhn.fr', '--filter', '=status:200'], '--filter cannot be used with the CDX files index'),
        (['--domain', 'mnhn.fr', '--processes', '2'], '--processes can only be used'),
        (['--domain', 'mnhn.fr', '-o', str(tmp_path / 'out'), '--limit', '1'], '--limit cannot be used'),
        (['--domain', 'mnhn.fr', '-o', str(tmp_path / 'out'), '--fields', 'url'], '--fields cannot be used'),
        (['--url-column', 'x', '--domain', 'mnhn.fr'], '--url-column and --url-file-type need --url-file'),
        (['--domain', 'mnhn.fr', '--limit', '0'], '--limit must be at least 1'),
        (['--domain', 'mnhn.fr', '--limit', '-1'], '--limit must be at least 1'),
        (['--domain', 'mnhn.fr', '--count', '--url-fgrep', 'x'], '--count cannot be used with --url-fgrep'),
    ]:
        with pytest.raises(SystemExit):
            main(['index', str(CDX_PATH)] + args)
        assert error in capsys.readouterr().err, args


@pytest.mark.parametrize('index', ['cc', 'cc-url-index-s3'])
def test_url_file_only_for_cdx_files(capsys, index):
    with pytest.raises(SystemExit):
        main(['index', index, '--url-file', 'urls.txt'])
    err = capsys.readouterr().err
    assert '--url-file cannot be used with the' in err
    if index != 'cc':
        assert 'JOIN' in err


def test_dry_run(capsys):
    main(['index', str(CDX_PATH), '--domain', 'mnhn.fr', '--url', 'example.com/a', '--dry-run'])
    out = capsys.readouterr().out
    assert '1 CDX file(s)' in out and 'prefix fr,mnhn)' in out and 'exact  com,example)/a' in out


@pytest.mark.parametrize(
    'entry, prefixes',
    [
        ('example.com', ['com,example)', 'com,example,']),
        ('https://example.com/', ['com,example)', 'com,example,']),
        ('example.com/reports', ['com,example)/reports']),
    ],
)
def test_domain_entry_to_surts(entry, prefixes):
    assert domain_entry_to_surts(entry) == prefixes


def test_url_file_type_domain():
    """With --url-file-type domain, host entries match their domain with subdomains (like #73's filter_cdx)."""
    url_list = str(TEST_DATA_PATH / 'repackage' / 'url_list.txt')
    backend = CdxFilesBackend(str(CDX_PATH))

    # e.g. 'bibliotheque.missiondefrance.fr' is a single URL as a pattern ...
    assert backend.count(IndexQuery(url_file=url_list)) == 0
    # ... and a domain with --url-file-type domain; 'biodiv.mnhn.fr/fr/taxonomy' is a URL prefix
    expected = count_lines('fr,missiondefrance,bibliotheque)') + count_lines('fr,mnhn,biodiv)/fr/taxonomy')
    assert backend.count(IndexQuery(url_file=url_list, url_file_type='domain')) == expected == len(LINES)


@pytest.mark.parametrize(
    'args, error',
    [
        (['--domain', 'mnhn.fr', '--url-file-type', 'domain'], '--url-column and --url-file-type need --url-file'),
        (['--url-file', 'urls.txt', '--url-file-type', 'regex'], "invalid choice: 'regex'"),
    ],
)
def test_url_file_type_errors(capsys, args, error):
    with pytest.raises(SystemExit):
        main(['index', str(CDX_PATH)] + args)
    assert error in capsys.readouterr().err


def test_no_globs_over_http(capsys):
    for command in (['index', 'https://example.org/indexes/cdx-*.gz', '--domain', 'mnhn.fr'],
                    ['repackage', 'https://example.org/indexes/cdx-*.gz']):
        with pytest.raises(SystemExit):
            main(command)
        assert 'globs do not work with http(s) URLs' in capsys.readouterr().err
