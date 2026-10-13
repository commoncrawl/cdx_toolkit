from argparse import ArgumentTypeError
import json
from unittest.mock import Mock, patch

import pytest

from warcio.archiveiterator import ArchiveIterator

import cdx_toolkit.cli.repackage
import cdx_toolkit.repackage.warc_records
import cdx_toolkit.warc
from cdx_toolkit.cli import main
from cdx_toolkit.cli.common import parse_size
from cdx_toolkit.cli.deprecated import get_command, rewrite_deprecated_command, translate
from cdx_toolkit.repackage.jobs import RepackageResult
import cdx_toolkit.cli.index


def _make_fake_record():
    resp = Mock()
    resp.status_code = 200
    resp.reason = 'OK'
    resp.headers = {'Content-Type': 'text/html'}
    resp.content = b'<html>test</html>'
    return cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=resp,
        capture={'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200'},
    )


RECORD = cdx_toolkit.repackage.warc_records.get_bytes_from_warc_record(_make_fake_record(), gzip=True)


def _range_response(offset=10):
    """The response to a range request for RECORD at `offset` of a WARC file."""
    content_range = f'bytes {offset}-{offset + len(RECORD) - 1}/*'
    return Mock(status_code=206, headers={'Content-Range': content_range}, content=RECORD)


def _write_captures(path, captures):
    path.write_text(''.join(json.dumps(c) + '\n' for c in captures))
    return str(path)


IA_CAPTURE = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200', 'mime': 'text/html'}
CC_CAPTURE = {
    'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200',
    'filename': 'crawl-data/x.warc.gz', 'offset': '10', 'length': str(len(RECORD)),
}


def test_repackage_with_wb_logs_revisit(tmp_path, caplog):
    captures = _write_captures(tmp_path / 'captures.jsonl', [dict(IA_CAPTURE, mime='warc/revisit')])
    prefix = str(tmp_path / 'out')

    with patch.object(cdx_toolkit.cli.repackage, 'fetch_wb_warc', return_value=_make_fake_record()) as fetch:
        with caplog.at_level('WARNING', logger='cdx_toolkit.cli.repackage'):
            main(['repackage', captures, '--wb', 'ia', '--prefix', prefix])

    assert fetch.call_args.kwargs['wb'] == 'https://web.archive.org/web'
    assert 'revisit record being resolved' in caplog.text
    assert list(tmp_path.glob('out-*.warc.gz'))


def test_repackage_fetches_by_warc_location(tmp_path):
    captures = _write_captures(tmp_path / 'captures.jsonl', [CC_CAPTURE])
    resp = _range_response()

    with patch('cdx_toolkit.repackage.jobs.myrequests_get', return_value=resp) as get:
        main(['repackage', captures, '--prefix', str(tmp_path / 'out'), '--warc-download-prefix', 'https://x.org/'])

    assert get.call_args.args == ('https://x.org/crawl-data/x.warc.gz',)
    assert get.call_args.kwargs['headers'] == {'Range': f'bytes=10-{9 + len(RECORD)}'}

    with open(tmp_path / 'out-001.warc.gz', 'rb') as f:
        records = list(ArchiveIterator(f))
    assert [r.rec_type for r in records] == ['warcinfo', 'response']


@pytest.mark.parametrize('existing, wb', [
    ('out-000000.extracted.warc.gz', True),
    ('out-001.warc.gz', True),
    ('out-000000.extracted.warc.gz', False),
])
def test_repackage_rejects_existing_warc_files_with_and_without_wb(tmp_path, capsys, existing, wb):
    """The same rule for both writers: no overwriting, no adding to the files of an earlier run."""
    (tmp_path / existing).write_bytes(b'earlier run')
    captures = _write_captures(tmp_path / 'captures.jsonl', [IA_CAPTURE if wb else CC_CAPTURE])
    wb_args = ['--wb', 'ia'] if wb else []

    with patch.object(cdx_toolkit.cli.repackage, 'fetch_wb_warc') as fetch, \
            patch('cdx_toolkit.repackage.jobs.myrequests_get') as get, \
            pytest.raises(SystemExit) as exc:
        main(['repackage', captures, '--prefix', str(tmp_path / 'out')] + wb_args)

    assert exc.value.code == 2
    assert 'exist already' in capsys.readouterr().err
    fetch.assert_not_called()
    get.assert_not_called()
    assert sorted(p.name for p in tmp_path.glob('out-*')) == [existing]
    assert (tmp_path / existing).read_bytes() == b'earlier run'


def test_repackage_without_warc_location_or_wb(tmp_path, capsys):
    captures = _write_captures(tmp_path / 'captures.jsonl', [CC_CAPTURE, IA_CAPTURE])

    with pytest.raises(SystemExit) as e:
        main(['repackage', captures, '--prefix', str(tmp_path / 'out')])

    assert e.value.code == 2
    assert 'use --wb' in capsys.readouterr().err
    assert not list(tmp_path.glob('out*'))  # nothing is written


def test_repackage_without_captures_writes_no_file(tmp_path):
    captures = _write_captures(tmp_path / 'captures.jsonl', [])

    main(['repackage', captures, '--prefix', str(tmp_path / 'out')])

    assert not list(tmp_path.glob('out*'))


def test_repackage_default_prefix(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    captures = _write_captures(tmp_path / 'captures.jsonl', [CC_CAPTURE])

    with patch('cdx_toolkit.repackage.jobs.myrequests_get', return_value=_range_response()):
        main(['repackage', captures])

    assert [p.name for p in tmp_path.glob('*.warc.gz')] == ['REPACKAGE-001.warc.gz']


def test_index_to_parquet_and_repackage(tmp_path, capsys):
    pytest.importorskip('pyarrow')
    captures = tmp_path / 'captures.parquet'

    with patch.object(cdx_toolkit.cli.index, 'resolve_index') as resolve_index:
        resolve_index.return_value.records.return_value = iter([CC_CAPTURE])
        main(['index', 'cc', '--url', 'example.com/*', '-o', str(captures)])
    with pytest.raises(SystemExit):
        main(['index', 'cc', '--url', 'example.com/*', '--output-format', 'parquet'])
    assert 'cannot be written to stdout' in capsys.readouterr().err

    with patch('cdx_toolkit.repackage.jobs.myrequests_get', return_value=_range_response()):
        main(['repackage', str(captures), '--prefix', str(tmp_path / 'out')])
    with open(tmp_path / 'out-001.warc.gz', 'rb') as f:
        assert [r.rec_type for r in ArchiveIterator(f)] == ['warcinfo', 'response']


def test_repackage_options_are_passed_to_the_warc_fetcher(tmp_path):
    captures = _write_captures(tmp_path / 'captures.jsonl', [])

    with patch('cdx_toolkit.repackage.pipeline.WARCFilter') as warc_filter:
        warc_filter.return_value.filter.return_value = RepackageResult()
        main([
            'repackage', captures, '--prefix', 'FOO', '--subprefix', 'BAR', '--size', '1MiB',
            '--creator', 'me', '--description', 'test', '--add-metadata-record', 'a.txt',
            '--parallel-readers', '4', '--warc-download-prefix', 's3://commoncrawl',
        ])

    kwargs = warc_filter.call_args.kwargs
    assert kwargs['prefix_path'] == 'FOO'
    assert kwargs['writer_subprefix'] == 'BAR'
    assert kwargs['max_file_size'] == 1024 * 1024
    assert kwargs['write_paths_as_metadata_records'] == ['a.txt']
    assert kwargs['n_parallel_readers'] == 4
    assert [job.url for job in kwargs['source'].iter_range_jobs()] == []
    info = kwargs['writer_info']
    assert info['isPartOf'] == 'FOO-BAR'
    assert info['creator'] == 'me'
    assert info['description'] == 'test'


def test_repackage_with_processes_uses_multiprocess(tmp_path):
    captures = _write_captures(tmp_path / 'captures.jsonl', [CC_CAPTURE])

    result = RepackageResult(1, 1)
    with patch('cdx_toolkit.repackage.multiprocess.run_multiprocess_repackage', return_value=result) as run:
        main(['repackage', captures, '--prefix', str(tmp_path / 'out'), '--processes', '3', '--subprefix', 'SUB'])

    kwargs = run.call_args.kwargs
    assert (kwargs['n_processes'], kwargs['writer_subprefix']) == (3, 'SUB')
    assert [job.filename for job in kwargs['source'].iter_range_jobs()] == ['crawl-data/x.warc.gz']


def test_repackage_with_wb_rejects_fetcher_options(tmp_path, capsys):
    captures = _write_captures(tmp_path / 'captures.jsonl', [IA_CAPTURE])

    with pytest.raises(SystemExit):
        main(['repackage', captures, '--wb', 'ia', '--processes', '2'])
    assert '--processes cannot be used with --wb' in capsys.readouterr().err


def test_repackage_with_wb_passes_size_and_warcinfo_to_writer(tmp_path):
    captures = _write_captures(tmp_path / 'captures.jsonl', [])

    with patch.object(cdx_toolkit.warc, 'get_writer', wraps=cdx_toolkit.warc.get_writer) as get_writer:
        main([
            'repackage', captures, '--wb', 'ia', '--prefix', 'FOO', '--subprefix', 'BAR', '--size', '1MiB',
            '--creator', 'me', '--description', 'test',
        ])

    prefix, subprefix, info = get_writer.call_args.args
    assert (prefix, subprefix) == ('FOO', 'BAR')
    assert get_writer.call_args.kwargs == {'size': 1024 * 1024}
    assert info['isPartOf'] == 'FOO-BAR'
    assert info['creator'] == 'me'
    assert info['description'] == 'test'


@pytest.mark.parametrize(
    'value, expected',
    [('1000', 1000), ('1kB', 1000), ('100MB', 100 * 1000 ** 2), ('1.5GB', 1500 * 1000 ** 2), ('1GiB', 1024 ** 3)],
)
def test_parse_size(value, expected):
    assert parse_size(value) == expected


@pytest.mark.parametrize('value', ['', 'GB', '1XB', '-1'])
def test_parse_size_invalid(value):
    with pytest.raises(ArgumentTypeError):
        parse_size(value)


@pytest.mark.parametrize(
    'args, command',
    [
        ('--cc --limit 10 iter example.com', 'iter'),
        ('--source https://example.com/cdx --crawl 3 size example.com', 'size'),
        ('-v index cc --url example.com', 'index'),
        ('--version', None),
    ],
)
def test_get_command(args, command):
    assert get_command(args.split()) == command


@pytest.mark.parametrize(
    'args, expected',
    [
        ('iter cc --url example.com/*', ['index', 'cc', '--url', 'example.com/*']),
        ('-v iter --help', ['-v', 'index', '--help']),
        # the old syntax is not translated, the arguments are the same as for `index`
        ('--cc --limit 10 iter example.com/*', ['--cc', '--limit', '10', 'index', 'example.com/*']),
    ],
)
def test_iter_is_an_alias_of_index(capsys, args, expected):
    assert rewrite_deprecated_command(args.split()) == expected
    assert '"iter" is deprecated' in capsys.readouterr().err


def test_iter_runs_like_index(capsys):
    with patch.object(cdx_toolkit.cli.index, 'run') as run:
        main(['iter', 'cc', '--url', 'example.com/*', '--limit', '3', '-o', 'out.csv'])

    args = run.call_args.args[0]
    assert (args.cmd, args.index, args.urls) == ('index', 'cc', ['example.com/*'])
    assert (args.limit, args.output) == (3, 'out.csv')
    assert '"iter" is deprecated' in capsys.readouterr().err


def test_translate_warc():
    args = '--source https://example.com/cdx --wb https://example.com/wb --limit=5 warc --size 1000 example.com/*'
    assert translate(args.split()) == (
        "cdxt index https://example.com/cdx --url 'example.com/*' --limit 5"
        " | cdxt repackage --wb https://example.com/wb --size 1000"
    )


def test_verbose_before_and_after_subcommand(tmp_path):
    captures = _write_captures(tmp_path / 'captures.jsonl', [])
    with patch.object(cdx_toolkit.cli, 'set_loglevel') as set_loglevel:
        main(['-v', 'repackage', captures, '--prefix', str(tmp_path / 'out')])
        assert set_loglevel.call_args.args == (1,)
        main(['repackage', '-vv', captures, '--prefix', str(tmp_path / 'out')])
        assert set_loglevel.call_args.args == (2,)


@pytest.mark.parametrize('n_failing', [1, 2])
def test_repackage_exits_1_if_captures_could_not_be_fetched(tmp_path, capsys, n_failing):
    captures = [dict(CC_CAPTURE, offset=str(i * len(RECORD))) for i in range(2)]
    path = _write_captures(tmp_path / 'captures.jsonl', captures)
    calls = []

    def get(url, headers=None, **kwargs):
        calls.append(url)
        if len(calls) <= n_failing:
            raise ConnectionError('network down')
        return _range_response(offset=int(headers['Range'][len('bytes='):].split('-')[0]))

    with patch('cdx_toolkit.repackage.jobs.myrequests_get', side_effect=get), \
            pytest.raises(SystemExit) as exc:
        main(['repackage', path, '--prefix', str(tmp_path / 'out')])
    assert exc.value.code == 1
    assert f'{n_failing} of 2 captures could not be fetched' in capsys.readouterr().err
    assert len(list(tmp_path.glob('out-*.warc.gz'))) == (1 if n_failing == 1 else 0)


def test_repackage_fails_if_the_output_cannot_be_created(tmp_path):
    path = _write_captures(tmp_path / 'captures.jsonl', [CC_CAPTURE])
    response = _range_response()
    with patch('cdx_toolkit.repackage.jobs.myrequests_get', return_value=response), \
            patch('cdx_toolkit.repackage.writers.local.aiofiles.open', side_effect=PermissionError('read-only')), \
            pytest.raises(PermissionError):
        main(['repackage', path, '--prefix', str(tmp_path / 'out')])
    assert list(tmp_path.glob('out-*')) == []


def test_repackage_with_wb_counts_failed_captures(tmp_path, capsys):
    path = _write_captures(tmp_path / 'captures.jsonl', [IA_CAPTURE, IA_CAPTURE])
    fetched = [RuntimeError('404'), _make_fake_record()]
    with patch.object(cdx_toolkit.cli.repackage, 'fetch_wb_warc', side_effect=fetched), \
            pytest.raises(SystemExit) as exc:
        main(['repackage', path, '--wb', 'ia', '--prefix', str(tmp_path / 'out')])
    assert exc.value.code == 1
    assert '1 of 2 captures could not be fetched' in capsys.readouterr().err
