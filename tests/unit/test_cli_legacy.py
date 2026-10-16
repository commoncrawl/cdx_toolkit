'''The command lines of cdx_toolkit 0.9.39 (cdx_toolkit/cli/legacy.py), without requests.'''
import json
import shlex
from unittest.mock import Mock, patch

import pytest

import cdx_toolkit.cli.legacy as legacy
from cdx_toolkit.cli import build_parser, main
from cdx_toolkit.cli.legacy import build_legacy_parser, is_legacy_command_line, suggested_replacement
from cdx_toolkit.repackage.jobs import RepackageResult


CAPTURES = [
    {'url': 'https://example.com/', 'status': '200', 'timestamp': '20240101000000', 'digest': 'A', 'mime': 'text/html'},
    {'url': 'https://example.com/b', 'status': '404', 'timestamp': '20240102000000', 'digest': 'B'},
]


def parse(cmdline: str):
    return build_legacy_parser().parse_args(shlex.split(cmdline))


@pytest.mark.parametrize(
    'args, expected',
    [
        ('--cc --limit 10 iter example.com/*', True),
        ('--crawl 2 iter example.com', True),
        ('--limit=5 iter example.com', True),
        ('-v -v --cc size example.com', True),
        ('--ia warc example.com', True),
        ('iter cc --url example.com', True),  # the unreleased alias: rejected by the old grammar
        ('--limit iter example.com', True),  # old options before the command: strict old grammar (error)
        ('-- iter example.com', True),
        ('index cc --url example.com', False),
        ('-v index cc --url iter', False),
        ('-vv repackage captures.jsonl', False),
        ('index cc --crawl iter --url example.com', False),
        ('--version', False),
        ('-h', False),
        ('', False),
    ],
)
def test_is_legacy_command_line(args, expected):
    assert is_legacy_command_line(args.split()) == expected


def test_a_value_of_an_old_option_is_not_the_command():
    # `--source iter`: iter is the value of --source, the command is size
    assert is_legacy_command_line(['--source', 'iter', 'size', 'x'])
    ns = parse('--source https://example.com/cdx --filter iter size x')
    assert (ns.cmd, ns.filter) == ('size', ['iter'])


@pytest.mark.parametrize(
    'args, message',
    [
        ('--cc iter', 'the following arguments are required: url'),
        ('--cc iter --url example.com', 'unrecognized arguments: --url'),
        ('iter cc --url example.com', 'unrecognized arguments: --url'),
        ('--limit iter example.com', "invalid int value: 'iter'"),
        ('--cc --limit', 'expected one argument'),
        ('--cc iter --size 3 example.com', 'unrecognized arguments: --size'),
        ('--limit 3 iter example.com', 'must specify --cc, --ia, or a --source'),
        ('--source example.com iter example.com', 'use the http(s) URL of a CDX API server'),
        ('--cc iter --all-fields --csv example.com', '--all-fields cannot be used with --csv'),
        ('--cc --wb https://example.com/wb warc example.com', 'cannot specify --wb for Common Crawl'),
        ('--source https://example.com/cdx warc example.com', 'needs --wb (a wayback machine) or'),
        ('--ia --wb ia warc --warc-download-prefix https://example.com example.com', 'cannot specify both'),
    ],
)
def test_invalid_old_command_lines_exit_2_without_requests(capsys, args, message):
    with patch.object(legacy, 'open_index') as open_index, patch.object(legacy, 'fetcher') as fetcher, \
            patch('cdx_toolkit.repackage.repackage') as repackage:
        with pytest.raises(SystemExit) as e:
            main(args.split())
    assert e.value.code == 2
    assert message in capsys.readouterr().err
    open_index.assert_not_called()
    fetcher.assert_not_called()
    repackage.assert_not_called()


@pytest.mark.parametrize(
    'args, source',
    [
        ('--crawl 1 --ia iter x', 'cc'),
        ('--cc --ia iter x', 'cc'),
        ('--source https://example.com/cdx --ia iter x', 'ia'),
        ('--source https://example.com/cdx iter x', 'https://example.com/cdx'),
    ],
)
def test_source_precedence(args, source):
    assert legacy.legacy_source(parse(args)) == source


def run_iter(capsys, args, captures=CAPTURES):
    """Run an old iter command line on `captures` from the CDX API backend."""
    backend = Mock()
    backend.records.return_value = iter(captures)
    with patch.object(legacy, 'open_index', return_value=backend) as open_index:
        main(args.split())
    out, err = capsys.readouterr()
    return out, err, open_index, backend


def test_iter_text_output(capsys):
    out, err, open_index, backend = run_iter(
        capsys, '--cc --crawl CC-MAIN-2024-33 --limit 2 --from 2024 --filter =status:200 iter example.com/*'
    )
    assert out.splitlines() == [
        'status 200, timestamp 20240101000000, url https://example.com/',
        'status 404, timestamp 20240102000000, url https://example.com/b',
    ]
    assert open_index.call_args.args == ('cc', 'cdx-api')
    query = backend.records.call_args.args[0]
    assert (query.urls, query.crawl, query.limit, query.from_ts, query.filters) == (
        ['example.com/*'], 'CC-MAIN-2024-33', 2, '2024', ['=status:200'])
    assert err.count('\n') == 1
    assert 'the old syntax, which is deprecated and will be removed in a future version' in err
    assert ("suggested replacement: cdxt index cc --url 'example.com/*' --crawl CC-MAIN-2024-33 --limit 2 "
            '--from 2024 --filter =status:200 --output-format csv --fields status,timestamp,url') in err


def test_iter_fields_are_deduplicated_and_missing_ones_omitted(capsys):
    out, _, _, _ = run_iter(capsys, '--ia iter --fields mime,url,mime x')
    assert out.splitlines() == ['mime text/html, url https://example.com/', 'url https://example.com/b']


def test_iter_all_fields(capsys):
    out, _, _, _ = run_iter(capsys, '--ia iter --fields url --all-fields x')
    assert out.splitlines()[0] == (
        'digest A, mime text/html, status 200, timestamp 20240101000000, url https://example.com/'
    )


def test_iter_jsonl(capsys):
    out, err, _, _ = run_iter(capsys, '--ia iter --jsonl x')
    assert out.splitlines()[0] == '{"status": "200", "timestamp": "20240101000000", "url": "https://example.com/"}'
    assert "cdxt index ia --url x --fields status,timestamp,url (see" in err


def test_iter_jsonl_all_fields(capsys):
    out, _, _, _ = run_iter(capsys, '--ia iter --jsonl --all-fields x')
    assert json.loads(out.splitlines()[1]) == CAPTURES[1]


def test_iter_csv(capsys):
    out, _, _, _ = run_iter(capsys, '--ia iter --csv --fields url,digest,mime x')
    assert out == 'digest,mime,url\r\nA,text/html,https://example.com/\r\nB,,https://example.com/b\r\n'


def test_iter_csv_header_without_captures(capsys):
    out, _, _, _ = run_iter(capsys, '--ia iter --csv x', captures=[])
    assert out == 'status,timestamp,url\r\n'


def test_iter_jsonl_and_csv(capsys):
    # as in 0.9.39: the CSV header, then JSON lines
    out, _, _, _ = run_iter(capsys, '--ia iter --csv --jsonl x')
    assert out.splitlines()[:2] == [
        'status,timestamp,url', '{"status": "200", "timestamp": "20240101000000", "url": "https://example.com/"}'
    ]


@pytest.mark.parametrize(
    'args, method, kwargs',
    [
        ('--cc --get iter x', 'get', {}),  # CDXFetcher.get() adds limit=1000
        ('--cc --get --limit 3 --closest 2017 iter x', 'get', {'limit': 3, 'closest': '2017'}),
        ('--cc --closest 2017 --to 2018 iter x', 'iter', {'closest': '2017', 'to': '2018'}),  # paged, as before
    ],
)
def test_iter_get_and_closest_use_the_fetcher(capsys, args, method, kwargs):
    cdx = Mock()
    getattr(cdx, method).return_value = CAPTURES[:1]
    with patch.object(legacy, 'fetcher', return_value=cdx), patch.object(legacy, 'open_index') as open_index:
        main(args.split())
    getattr(cdx, method).assert_called_once_with('x', **kwargs)
    open_index.assert_not_called()
    out, err = capsys.readouterr()
    assert out == 'status 200, timestamp 20240101000000, url https://example.com/\n'
    if args == '--cc --get iter x':
        assert 'cdxt index cc --url x --limit 1000 --output-format csv' in err


def test_fetcher_options():
    with patch('cdx_toolkit.CDXFetcher') as fetcher:
        legacy.fetcher(parse('--crawl 2 --cc-mirror https://m/ --cc-sort ascending iter x'), 'cc')
        assert fetcher.call_args.kwargs == {
            'source': 'cc', 'crawl': ['2'], 'cc_mirror': 'https://m/', 'cc_sort': 'ascending'}
        legacy.fetcher(parse('--ia --cc-mirror https://m/ iter x'), 'ia')  # ignored for other sources, as before
        assert fetcher.call_args.kwargs == {'source': 'ia'}


def test_size_is_not_capped_by_the_limit(capsys):
    cdx = Mock()
    cdx.get_size_estimate.return_value = 1500
    with patch.object(legacy, 'fetcher', return_value=cdx):
        main('--cc --limit 10 size --details x'.split())
    cdx.get_size_estimate.assert_called_once_with('x', limit=10, details=True)
    out, err = capsys.readouterr()
    assert out == '1500\n'
    assert "suggested replacement: cdxt index cc --url x --limit 10 --count" in err


def run_warc(args, result=RepackageResult(2, 2, 0), captures=CAPTURES):
    with patch.object(legacy, 'legacy_captures', return_value=iter(captures)) as legacy_captures, \
            patch('cdx_toolkit.repackage.repackage', return_value=result) as repackage:
        main(args.split())
    return legacy_captures, repackage


def test_warc_streams_into_repackage(capsys):
    legacy_captures, repackage = run_warc(
        '--cc --get --limit 2 warc --subprefix SUB --size 1000 --creator me --url-fgrepv /b x')
    assert legacy_captures.call_args.kwargs == {'use_get': False}  # 0.9.39 iterated with --get too
    args, kwargs = repackage.call_args
    assert list(args[0]) == CAPTURES[:1]  # --url-fgrepv after the limit
    assert args[1] == 'TEST'
    assert kwargs['sort'] is False
    assert kwargs['warc_download_prefix'] == 'https://data.commoncrawl.org' and 'wb' not in kwargs
    assert (kwargs['subprefix'], kwargs['size'], kwargs['creator'], kwargs['operator']) == ('SUB', 1000, 'me', None)
    assert kwargs['description'] == (
        'warc extraction generated with: cdxt --cc --get --limit 2 warc --subprefix SUB --size 1000 --creator me '
        '--url-fgrepv /b x'
    )
    assert ('suggested replacement: cdxt index cc --url x --limit 2 --url-fgrepv /b | cdxt repackage --prefix TEST '
            '--subprefix SUB --size 1000 --creator me --no-sort') in capsys.readouterr().err


@pytest.mark.parametrize(
    'args, content, suggestion',
    [
        ('--ia warc x', {'wb': 'ia'}, '| cdxt repackage --prefix TEST --wb ia --no-sort'),
        ('--ia warc --warc-download-prefix https://w x', {'warc_download_prefix': 'https://w'},
         '--warc-download-prefix https://w --no-sort'),
        ('--source https://e/cdx --wb https://e/wb warc x', {'wb': 'https://e/wb'}, '--wb https://e/wb --no-sort'),
        ('--crawl 1 warc --warc-download-prefix s3://commoncrawl x', {'warc_download_prefix': 's3://commoncrawl'},
         '--warc-download-prefix s3://commoncrawl --no-sort'),
    ],
)
def test_warc_content_source(capsys, args, content, suggestion):
    _, repackage = run_warc(args)
    kwargs = repackage.call_args.kwargs
    assert {k: kwargs[k] for k in ('wb', 'warc_download_prefix') if k in kwargs} == content
    assert suggestion in capsys.readouterr().err


def test_warc_exits_1_if_captures_failed(capsys):
    with pytest.raises(SystemExit) as e:
        run_warc('--cc warc x', result=RepackageResult(2, 1, 1))
    assert e.value.code == 1
    assert '1 of 2 captures could not be fetched' in capsys.readouterr().err


@pytest.mark.parametrize(
    'args',
    [
        '--cc --limit 10 iter example.com/*',
        "--crawl 'CC-MAIN-2024-33,CC-MAIN-2024-30' --cc-sort ascending --from 2024 --to 20240410 "
        "--filter '!=status:200' --filter =mime:text/html iter --fields url,digest example.com/*",
        '--ia --get --closest 2017 iter --jsonl --all-fields example.com',
        '--source https://example.com/cdx iter --csv example.com',
        '--cc --limit 3 size --details example.com',
        "--ia warc --prefix 'my dir/IA' --subprefix X --size 100 --creator 'a b' --operator c --url-fgrep a "
        '--url-fgrepv robots.txt example.com/*',
        '--cc --cc-mirror https://index.commoncrawl.org/ warc --warc-download-prefix s3://commoncrawl example.com',
    ],
)
def test_suggested_replacement_is_a_valid_new_command_line(args):
    suggestion = suggested_replacement(parse(args))
    for part in suggestion.split(' | '):
        tokens = shlex.split(part)
        assert tokens[0] == 'cdxt'
        build_parser().parse_args(tokens[1:])  # exits on an error


def test_legacy_captures_without_a_crawl_or_limit():
    backend = Mock()
    with patch.object(legacy, 'open_index', return_value=backend) as open_index:
        legacy.legacy_captures(parse('--source https://example.com/cdx.php iter x'), 'https://example.com/cdx.php',
                               use_get=False)
    assert open_index.call_args.args == ('https://example.com/cdx.php', 'cdx-api')  # not detected from the URL
    query = backend.records.call_args.args[0]
    assert (query.crawl, query.limit, query.filters) == (None, None, [])
