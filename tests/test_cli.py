import json
import sys
import os
import platform
import logging
import re

import pytest
import requests
import responses

from cdx_toolkit.cli import main

from tests.conftest import conditional_mock_responses


LOGGER = logging.getLogger(__name__)


def slow_ci():
    """
    For Github Actions, the windows and macos runners are very slow.
    Detect those runners, so that we can cut testing short.
    """
    if os.environ.get('CDXT_FAKE_GITHUB_ACTION'):
        LOGGER.error('limiting pytest because CDXT_FAKE_GITHUB_ACTION')
        return True
    if os.environ.get('GITHUB_ACTION'):
        if platform.system() in {'Darwin', 'Windows'}:
            LOGGER.error('limiting pytest because GITHUB_ACTION')
            return True
    v = sys.version_info
    if os.environ.get('GITHUB_ACTION') and v.major == 3 and v.minor != 12:
        LOGGER.error('limiting pytest because GITHUB_ACTION and py != 3.12')
        return True
    LOGGER.error('full pytest')


@conditional_mock_responses
def test_basics(capsys):
    args = 'index cc --limit 10 --url commoncrawl.org/*'.split()
    main(args=args)
    out, err = capsys.readouterr()

    split = out.splitlines()
    assert len(split) == 10
    for line in out.splitlines():
        # this might be commoncrawl.org./ or commoncrawl.org/
        assert 'commoncrawl.org' in line


@conditional_mock_responses
def test_basics_2(capsys):
    args = 'index cc --crawl 2 --limit 10 --url commoncrawl.org/*'.split()
    main(args=args)
    out, err = capsys.readouterr()

    split = out.splitlines()
    assert len(split) == 10
    for line in out.splitlines():
        # this might be commoncrawl.org./ or commoncrawl.org/
        assert 'commoncrawl.org' in line


def multi_helper(t, capsys, caplog):
    cmdline, outputs = t
    args = cmdline.split()

    if 'exception' in outputs:
        with pytest.raises(outputs['exception']):
            main(args=args)
    else:
        main(args=args)

    out, err = capsys.readouterr()

    assert err == '', cmdline
    lines = out.splitlines()
    if 'count' in outputs:
        assert len(lines) == outputs['count'], cmdline
    for line in lines:
        if 'linefgrep' in outputs:
            assert outputs['linefgrep'] in line, cmdline
        if 'linefgrepv' in outputs:
            assert outputs['linefgrepv'] not in line, cmdline
        if 'csv' in outputs:
            assert line.count(',') >= 2, cmdline
        if 'jsonl' in outputs:
            assert line.startswith('{') and line.endswith('}'), cmdline
            assert json.loads(line), cmdline
        if 'is_int' in outputs:
            assert line.isdigit(), cmdline

    if 'debug' in outputs:
        assert len(caplog.records) > outputs['debug'], cmdline


@conditional_mock_responses
def test_multi_cc1(capsys, caplog):
    # this is the test case before slow_ci -> break
    testdata = ('index cc --limit 10 --url commoncrawl.org/*', {'count': 10, 'linefgrep': 'commoncrawl.org'})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_1(capsys, caplog):
    testdata = ('index cc --limit 11 --url commoncrawl.org/*', {'count': 11, 'linefgrep': 'commoncrawl.org'})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_2(capsys, caplog):
    # runs slowly if we don't limit crawl to 1
    testdata = ('index cc --crawl 1 --limit 10 --url commoncrawl.org/thisurlneverdidexist', {'count': 0})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_3(capsys, caplog):
    testdata = (
        'index cc --cc-mirror https://index.commoncrawl.org/ --limit 11 --url commoncrawl.org/*',
        {'count': 11, 'linefgrep': 'commoncrawl.org'},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_4(capsys, caplog):
    # all fields are output by default
    testdata = ('index cc --limit 10 --url commoncrawl.org/*', {'count': 10, 'linefgrep': '"digest": '})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_5(capsys, caplog):
    testdata = (
        'index cc --limit 10 --url commoncrawl.org/* --fields digest,length,offset --output-format csv',
        {'count': 11, 'csv': True},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_6(capsys, caplog):
    testdata = (
        'index cc --limit 10 --filter=status:200 --url commoncrawl.org/*',
        {'count': 10, 'linefgrep': '"status": "200"'},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_7(capsys, caplog):
    testdata = (
        'index cc --limit 10 --filter=!status:200 --filter=!status:404 --url commoncrawl.org/*',
        {'count': 10, 'linefgrepv': '"status": "200"'},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_8(capsys, caplog):
    testdata = (
        'index cc --limit 10 --to=2017 --url commoncrawl.org/*',
        {'count': 10, 'linefgrep': '"timestamp": "2017'},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc1_slow_9(capsys, caplog):
    testdata = (
        'index cc --limit 10 --from=2017 --to=2017 --url commoncrawl.org/*',
        {'count': 10, 'linefgrep': '"timestamp": "2017'},
    )
    multi_helper(testdata, capsys, caplog)


@conditional_mock_responses
def test_multi_cc2(capsys, caplog):
    # this is the test case before slow_ci -> break
    testdata = (
        'index cc --limit 3 --closest=20170615 --url commoncrawl.org/*',
        {'count': 3, 'linefgrep': '"timestamp": "20170'},
    )  # data-dependent, and kinda broken
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc2_slow_1(capsys, caplog):
    testdata = (
        'index cc --limit 3 --filter status:200 --closest=20170615 --url commoncrawl.org/*',
        {'count': 3, 'linefgrep': '"timestamp": "20170'},
    )  # data-dependent, and kinda broken
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc2_slow_2(capsys, caplog):
    testdata = ('index cc --closest=20170615 --url commoncrawl.org/never-existed', {'count': 0})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc2_slow_3(capsys, caplog):
    testdata = ('index cc --limit 10 --url commoncrawl.org/* --output-format csv', {'count': 11, 'csv': True})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc2_slow_4(capsys, caplog):
    testdata = ('index cc --limit 10 --url commoncrawl.org/* --output-format jsonl', {'count': 10, 'jsonl': True})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_cc2_slow_5(capsys, caplog):
    testdata = ('index cc -v -v --limit 10 --url commoncrawl.org/*', {'count': 10, 'debug': 5})
    multi_helper(testdata, capsys, caplog)


@conditional_mock_responses
def test_multi_ia(capsys, caplog):
    testdata = ('index ia --limit 10 --url commoncrawl.org/*', {'count': 10, 'linefgrep': 'commoncrawl.org'})
    # Disabled: minimize IA for ratelimit purposes
    # ('index ia --limit 10 --url commoncrawl.org/thisurlneverdidexist', {'count': 0}),
    # ('index ia --limit 10 --url commoncrawl.org/*', {'count': 10, 'linefgrep': '"mime": '}),  # renamed field
    # ('index ia --limit 4 --closest=20170615 --url commoncrawl.org/*',
    #  {'count': 4, 'linefgrep': '"timestamp": '}),  # returns 2008 ?! bug probably on my end
    # ('index ia -v -v --limit 10 --url commoncrawl.org/*', {'count': 10, 'debug': 5}),
    multi_helper(testdata, capsys, caplog)


@responses.activate
def test_multi_misc_not_ia(capsys, caplog):
    # this is the test case before slow_ci -> break; the hostname does not exist (no DNS request: mocked)
    dns_error = requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='web.arc4567hive.org', port=443): Failed to resolve 'web.arc4567hive.org' "
        '([Errno 8] nodename nor servname provided, or not known)'
    )
    responses.add(responses.GET, re.compile(r'https://web\.arc4567hive\.org/.*'), body=dns_error)
    testdata = (
        '-v -v index https://web.arc4567hive.org/cdx/search/cdx --limit 10 --url commoncrawl.org/*',
        {'exception': ValueError},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_misc_not_ia_slow_1(capsys, caplog):
    testdata = (
        '-v -v index https://example.com/404 --limit 10 --url commoncrawl.org/*',
        {'exception': ValueError},
    )
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_misc_not_ia_slow_2(capsys, caplog):
    testdata = ('index cc --crawl 1,1 --limit 10 --url commoncrawl.org/*', {'exception': ValueError})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_misc_not_ia_slow_3(capsys, caplog):
    testdata = ('index cc --crawl 1,CC-MAIN-2024 --limit 10 --url commoncrawl.org/*', {'exception': ValueError})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_multi_misc_not_ia_slow_4(capsys, caplog):
    testdata = ('index cc --limit 10 --url commoncrawl.org/* --count', {'count': 1, 'is_int': True})
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
def test_multi_misc_not_ia_slow_6(capsys, caplog):
    # INDEX is required
    with pytest.raises(SystemExit):
        main(args='index --limit 10 --url commoncrawl.org/*'.split())


@conditional_mock_responses
def test_multi_misc_ia(capsys, caplog):
    testdata = (
        'index https://web.archive.org/cdx/search/cdx --limit 10 --url commoncrawl.org/*',
        {'count': 10, 'linefgrep': 'commoncrawl.org'},
    )
    # Disabled: minimize IA for ratelimit reasons
    # ('index ia --limit 10 --url commoncrawl.org/* --count', {'count': 1, 'is_int': True}),
    # ('index ia --from 20180101 --to 20180110 --limit 10 --url commoncrawl.org --count', {'count': 1}),
    multi_helper(testdata, capsys, caplog)


def warc_test_helper(tmpdir, index_args: str, repackage_args: str = ''):
    with tmpdir.as_cwd():
        for cmdline in (
            f'{index_args} -o captures.jsonl',
            f'repackage captures.jsonl {repackage_args}',
        ):
            print(cmdline, file=sys.stderr)
            main(args=cmdline.split())


@conditional_mock_responses
def test_warc_prefix_1(tmpdir):
    warc_test_helper(tmpdir, '-v -v index cc --limit 2 --url commoncrawl.org/*')
    assert tmpdir.listdir(fil='REPACKAGE-001.warc.gz')


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_warc_prefix_2(tmpdir):
    warc_test_helper(tmpdir, 'index ia --limit 1 --url commoncrawl.org/*', '--wb ia')
    assert tmpdir.listdir(fil='REPACKAGE-*.warc.gz')


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_warc_prefix_3(tmpdir):
    warc_test_helper(tmpdir, 'index cc --cc-mirror https://index.commoncrawl.org/ --limit 2 --url commoncrawl.org/*')
    assert tmpdir.listdir(fil='REPACKAGE-001.warc.gz')


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_warc_prefix_4(tmpdir):
    warc_test_helper(
        tmpdir,
        'index https://web.archive.org/cdx/search/cdx --limit 1 --url commoncrawl.org/*',
        '--wb https://web.archive.org/web',
    )
    assert tmpdir.listdir(fil='REPACKAGE-*.warc.gz')


@conditional_mock_responses
def test_warc_suffix_1(tmpdir):
    warc_test_helper(
        tmpdir,
        '-v -v index cc --limit 1 --url-fgrep common --url-fgrepv bar --url commoncrawl.org/*',
        '--prefix FOO --subprefix BAR --size 1 --creator creator --operator bob',
    )
    assert tmpdir.listdir(fil='FOO-BAR-001.warc.gz')


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_warc_suffix_2(tmpdir):
    warc_test_helper(
        tmpdir, '-v -v index cc --limit 1 --url-fgrep bar --url commoncrawl.org/*', '--prefix EMPTY --size 1'
    )
    assert not tmpdir.listdir(fil='EMPTY-*')  # no WARC file without records


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_warc_suffix_3(tmpdir):
    warc_test_helper(
        tmpdir, '-v -v index cc --limit 1 --url-fgrepv common --url commoncrawl.org/*', '--prefix EMPTY --size 1'
    )
    assert not tmpdir.listdir(fil='EMPTY-*')  # no WARC file without records


@pytest.mark.skipif(slow_ci(), reason='Slow CI')
@conditional_mock_responses
def test_warc_suffix_4(tmpdir):
    warc_test_helper(
        tmpdir,
        '-v -v index cc --limit 1 --url-fgrep common --url-fgrepv bar --url commoncrawl.org/*',
        '--prefix FOO --subprefix BAR --size 1 --creator creator --operator bob',
    )
    assert tmpdir.listdir(fil='FOO-BAR-001.warc.gz')


def one_ia_corner(tmpdir, index_args):
    warc_test_helper(tmpdir, 'index ia ' + index_args, '--wb ia')


@pytest.mark.skip(reason='needs some ratelimit love')
def test_warc_ia_corners(tmpdir, caplog):
    """
    To test these more properly, need to add a --exact-warcname and then postprocess.
    For now, these are only crash tests.
    """

    # revisit vivification
    one_ia_corner(tmpdir, '--from 2017010118350 --to 2017010118350 --url pbm.com/robots.txt')

    # same-surt same-timestamp redir+200
    one_ia_corner(tmpdir, '--from 20090220001146 --to 20090220001146 --url pbm.com')

    # any redir -> 302, this is a 301
    one_ia_corner(tmpdir, '--from 2011020713024 --to 2011020713024 --url pbm.com')

    # warcing a 404 is a corner case in myrequests
    one_ia_corner(
        tmpdir, '--from 20080512074145 --to 20080512074145 --url http://www.pbm.com/oly/archive/design94/0074.html'
    )
