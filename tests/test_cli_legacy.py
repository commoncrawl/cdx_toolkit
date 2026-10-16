'''The command lines of cdx_toolkit 0.9.39 (deprecated), with the responses recorded for them: the mock data of
tests/test_cli.py was recorded with these command lines (`--cc --limit 10 iter URL` makes the same requests as
`index cc --limit 10 --url URL`).'''
import functools
import json
import re

import pytest
import responses
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.cli import main

from tests.conftest import LIVE_TESTS, conditional_mock_responses, mock_response_from_jsonl
from tests.test_cli import slow_ci


DEPRECATED = 'this command line uses the old syntax, which is deprecated'
TEXT_LINE = re.compile(r'status \d+, timestamp \d{14}, url \S+')


def recorded(name):
    """The recorded responses of tests/test_cli.py::<name> (unless CDXT_LIVE_TESTS has http)."""
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if 'http' not in LIVE_TESTS:
                mock_response_from_jsonl(name, 'test_cli')
            return f(*args, **kwargs)

        return conditional_mock_responses(auto_mock_data=False)(wrapper)

    return decorator


def run(capsys, cmdline):
    main(args=cmdline.split())
    out, err = capsys.readouterr()
    assert DEPRECATED in err and 'suggested replacement: cdxt ' in err
    return out, err


@recorded('test_basics')
def test_basics(capsys):
    out, err = run(capsys, '--cc --limit 10 iter commoncrawl.org/*')
    lines = out.splitlines()
    assert len(lines) == 10
    for line in lines:
        assert TEXT_LINE.fullmatch(line) and 'commoncrawl.org' in line
    assert ("cdxt index cc --url 'commoncrawl.org/*' --limit 10 --output-format csv --fields status,timestamp,url"
            in err)


@recorded('test_basics_2')
def test_basics_2(capsys):
    out, _ = run(capsys, '--crawl 2 --limit 10 iter commoncrawl.org/*')
    assert len(out.splitlines()) == 10


@recorded('test_multi_cc1_slow_4')
def test_all_fields(capsys):
    out, _ = run(capsys, '--cc --limit 10 iter commoncrawl.org/* --all-fields')
    lines = out.splitlines()
    assert len(lines) == 10 and all('digest ' in line and 'filename crawl-data/' in line for line in lines)


@recorded('test_multi_cc1_slow_5')
def test_csv(capsys):
    out, _ = run(capsys, '--cc --limit 10 iter commoncrawl.org/* --fields=digest,length,offset,digest --csv')
    lines = out.split('\r\n')
    assert lines[0] == 'digest,length,offset'
    assert len(lines) == 12 and lines[-1] == ''  # header, 10 captures, CRLF line endings
    assert all(line.count(',') == 2 for line in lines[:-1])


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@recorded('test_multi_cc2_slow_4')
def test_jsonl(capsys):
    out, _ = run(capsys, '--cc --limit 10 iter commoncrawl.org/* --jsonl')
    lines = out.splitlines()
    assert len(lines) == 10
    assert all(list(json.loads(line)) == ['status', 'timestamp', 'url'] for line in lines)


@recorded('test_multi_cc1_slow_7')
def test_filters(capsys):
    out, _ = run(capsys, '--cc --limit 10 --filter=!status:200 --filter=!status:404 iter commoncrawl.org/*')
    lines = out.splitlines()
    assert len(lines) == 10 and not any(line.startswith(('status 200,', 'status 404,')) for line in lines)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@recorded('test_multi_cc1_slow_9')
def test_from_to(capsys):
    out, _ = run(capsys, '--cc --limit 10 --from=2017 --to=2017 iter commoncrawl.org/*')
    lines = out.splitlines()
    assert len(lines) == 10 and all(', timestamp 2017' in line for line in lines)


@recorded('test_multi_cc2')
def test_get_closest(capsys):
    out, _ = run(capsys, '--cc --limit 3 --get --closest=20170615 iter commoncrawl.org/*')
    lines = out.splitlines()
    assert len(lines) == 3 and all(', timestamp 20170' in line for line in lines)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@recorded('test_multi_cc2_slow_2')
def test_get_without_limit(capsys):
    # the recorded requests have limit=1000, the default of a single get
    out, err = run(capsys, '--cc --get --closest=20170615 iter commoncrawl.org/never-existed')
    assert out == ''
    assert '--limit 1000 --closest 20170615' in err


@recorded('test_multi_ia')
def test_ia(capsys):
    out, _ = run(capsys, '--ia --limit 10 iter commoncrawl.org/*')
    assert len(out.splitlines()) == 10


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@recorded('test_multi_misc_ia')
def test_source(capsys):
    out, _ = run(capsys, '--source https://web.archive.org/cdx/search/cdx --limit 10 iter commoncrawl.org/*')
    assert len(out.splitlines()) == 10


@recorded('test_multi_misc_not_ia_slow_4')
def test_size(capsys):
    out, err = run(capsys, '--cc --limit 10 size commoncrawl.org/*')
    assert out.strip().isdigit()
    assert "cdxt index cc --url 'commoncrawl.org/*' --limit 10 --count" in err


@responses.activate
def test_size_details(capsys):
    # one CDX API server: the estimate of the index, then the total (not capped by --limit)
    responses.add(responses.GET, 'https://example.com/cdx', json={'blocks': 3})
    out, _ = run(capsys, '--source https://example.com/cdx --limit 10 size --details commoncrawl.org/*')
    assert out == 'https://example.com/cdx 6000\n6000\n'
    assert responses.calls[0].request.params == {'url': 'commoncrawl.org/*', 'showNumPages': 'true', 'limit': '10'}


def warc_records(path):
    """(type, content) of the records of a WARC file."""
    with open(path, 'rb') as f:
        return [(record.rec_type, record.content_stream().read().decode(errors='replace'))
                for record in ArchiveIterator(f)]


@recorded('test_warc_prefix_1')
def test_warc_cc(tmpdir, capsys):
    with tmpdir.as_cwd():
        _, err = run(capsys, '-v -v --cc --limit 2 warc commoncrawl.org/*')
    assert [p.basename for p in tmpdir.listdir()] == ['TEST-001.warc.gz']
    records = warc_records(tmpdir / 'TEST-001.warc.gz')
    assert [rec_type for rec_type, _ in records] == ['warcinfo', 'response', 'response']
    warcinfo = records[0][1]
    assert 'isPartOf: TEST' in warcinfo
    assert 'description: warc extraction generated with: cdxt -v -v --cc --limit 2 warc commoncrawl.org/*' in warcinfo
    assert "cdxt index cc --url 'commoncrawl.org/*' --limit 2 | cdxt repackage --prefix TEST --no-sort" in err


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@recorded('test_warc_prefix_2')
def test_warc_ia(tmpdir, capsys):
    # replayed from the Internet Archive's wayback machine, without --wb
    with tmpdir.as_cwd():
        _, err = run(capsys, '--ia --limit 1 warc commoncrawl.org/*')
    assert [p.basename for p in tmpdir.listdir()] == ['TEST-001.warc.gz']
    assert [rec_type for rec_type, _ in warc_records(tmpdir / 'TEST-001.warc.gz')] == ['warcinfo', 'response']
    assert '| cdxt repackage --prefix TEST --wb ia --no-sort' in err


@recorded('test_warc_suffix_1')
def test_warc_options(tmpdir, capsys):
    with tmpdir.as_cwd():
        run(capsys, '-v -v --cc --limit 1 warc --prefix FOO --subprefix BAR --size 1 --creator creator '
                    '--operator bob --url-fgrep common --url-fgrepv bar commoncrawl.org/*')
    assert [p.basename for p in tmpdir.listdir()] == ['FOO-BAR-001.warc.gz']
    records = warc_records(tmpdir / 'FOO-BAR-001.warc.gz')
    warcinfo = records[0][1]
    assert 'isPartOf: FOO-BAR' in warcinfo and 'creator: creator' in warcinfo and 'operator: bob' in warcinfo


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@recorded('test_warc_suffix_2')
def test_warc_empty_selection(tmpdir, capsys):
    with tmpdir.as_cwd():
        run(capsys, '-v -v --cc --limit 1 warc --prefix EMPTY --size 1 --url-fgrep bar commoncrawl.org/*')
    assert tmpdir.listdir() == []  # no WARC file without captures
