import os
import unittest.mock as mock
import pytest

import cdx_toolkit.commoncrawl
from cdx_toolkit.timeutils import timestamp_to_time

from tests.conftest import conditional_mock_responses

# useful for debugging:
import logging

logging.basicConfig(level='INFO')


def test_normalize_crawl():
    tests = [
        [['1'], ['1']],
        [['a'], ['a']],
        [['a', 'b'], ['a', 'b']],
        [['a,b', 'c'], ['a', 'b', 'c']],
        [['a,b,c,d'], ['a', 'b', 'c', 'd']],
        [['a', 'b,c'], ['a', 'b', 'c']],
    ]

    for t in tests:
        assert cdx_toolkit.commoncrawl.normalize_crawl(t[0]) == t[1]
    with pytest.raises(ValueError):
        cdx_toolkit.commoncrawl.normalize_crawl(['1', '2'])


def test_apply_cc_defaults():
    # no from
    #  closest -- sets from, to
    #  to -- sets from
    #  else -- sets from and not to

    # at this point, we're guaranteed from is set

    # no to
    #  closest -- there was from AND closest and no to, now I set to
    #  else -- no to set at all, ok

    now = 1524962339.157388  # 20180429003859

    tests = [
        [{'crawl': 'foo'}, {}],
        [{'closest': '20180101'}, {'from_ts': '20171003000000', 'to': '20180401000000'}],
        [{'closest': '20180101', 'to': '20181201'}, {'from_ts': '20171003000000'}],
        [{'to': '20180101'}, {'from_ts': '20170101235959'}],  # one year before the end of 2018-01-01
        [{}, {'from_ts': '20170429003859'}],  # uses now
        [{'from_ts': '20100101', 'closest': '20150301'}, {'to': '20150530000000'}],
        [{'from_ts': '20100101'}, {}],
    ]

    for test_in, test_out in tests:
        crawl_present = bool(test_in.pop('crawl', None))
        test_out.update(test_in)
        cdx_toolkit.commoncrawl.apply_cc_defaults(test_in, crawl_present=crawl_present, now=now)
        assert test_in == test_out


my_cc_endpoints = [
    # expected to be ascending
    'https://index.commoncrawl.org/CC-MAIN-2008-2009-index',
    'https://index.commoncrawl.org/CC-MAIN-2009-2010-index',
    'https://index.commoncrawl.org/CC-MAIN-2012-index',
    'https://index.commoncrawl.org/CC-MAIN-2013-20-index',
    'https://index.commoncrawl.org/CC-MAIN-2017-51-index',
    'https://index.commoncrawl.org/CC-MAIN-2018-05-index',
    'https://index.commoncrawl.org/CC-MAIN-2018-09-index',
    'https://index.commoncrawl.org/CC-MAIN-2018-13-index',
]


def test_match_cc_crawls():
    tests = [
        [['CC-MAIN-2013-20'], ['https://index.commoncrawl.org/CC-MAIN-2013-20-index']],
        [['CC-MAIN-2017'], ['https://index.commoncrawl.org/CC-MAIN-2017-51-index']],
        [
            ['CC-MAIN-2018'],
            [
                'https://index.commoncrawl.org/CC-MAIN-2018-05-index',
                'https://index.commoncrawl.org/CC-MAIN-2018-09-index',
                'https://index.commoncrawl.org/CC-MAIN-2018-13-index',
            ],
        ],
        [
            ['CC-MAIN-2013', 'CC-MAIN-2017'],
            [
                'https://index.commoncrawl.org/CC-MAIN-2013-20-index',
                'https://index.commoncrawl.org/CC-MAIN-2017-51-index',
            ],
        ],
        [['CC-MAIN-2013-20', 'no match'], ['https://index.commoncrawl.org/CC-MAIN-2013-20-index']],  # .warning
    ]
    for t in tests:
        assert cdx_toolkit.commoncrawl.match_cc_crawls(t[0], my_cc_endpoints) == t[1]
    with pytest.raises(ValueError):
        cdx_toolkit.commoncrawl.match_cc_crawls(['no match'], my_cc_endpoints)


def test_make_cc_maps():
    cc_map, cc_times = cdx_toolkit.commoncrawl.make_cc_maps(my_cc_endpoints)
    t = cc_times[0]
    assert cc_map[t] == 'https://index.commoncrawl.org/CC-MAIN-2008-2009-index'
    t = cc_times[-1]
    assert cc_map[t] == 'https://index.commoncrawl.org/CC-MAIN-2018-13-index'


def test_check_cc_from_to():
    params_that_raise = [
        {'closest': '2010', 'to': '2010'},  # needs from
        {'closest': '2010', 'from_ts': '2010'},  # needs to
        {'to': '2010'},  # needs from_ts
        {},  # needs 'from_ts'
    ]
    for params in params_that_raise:
        with pytest.raises(ValueError):
            cdx_toolkit.commoncrawl.check_cc_from_to(params)


def test_bisect_cc():
    cc_map, cc_times = cdx_toolkit.commoncrawl.make_cc_maps(my_cc_endpoints)

    tests = [
        # [(from, to), (first, last, count)],
        [('201801', '201804'), ('2017-51', '2018-13', 4)],  # XXX one too many at start
        [('20180214', '201804'), ('2018-05', '2018-13', 3)],  # XXX one too many at start
        [
            ('20180429', '20180430'),
            ('2018-13', '2018-13', 1),
        ],  # XXX one too early for start and end XXX should be visible in cli... cli shows 2018-{17,13,9}
        # [('', ''), ('', '', 1)],
    ]

    i_last = sorted(my_cc_endpoints)[-1]

    for t in tests:
        from_ts_t = timestamp_to_time(t[0][0])
        to_t = timestamp_to_time(t[0][1])
        i_from = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][0])
        i_to = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][1])
        i_count = t[1][2]

        index_list = cdx_toolkit.commoncrawl.bisect_cc(cc_map, cc_times, from_ts_t, to_t)
        assert index_list[0] == i_from, 'test: ' + repr(t)
        assert index_list[-1] == i_to, 'test: ' + repr(t)
        assert len(index_list) == i_count

        index_list = cdx_toolkit.commoncrawl.bisect_cc(cc_map, cc_times, from_ts_t, None)
        assert index_list[0] == i_from, 'test: ' + repr(t)
        assert index_list[-1] == i_last, 'test: ' + repr(t)
        assert len(index_list) >= i_count


def test_customize_index_list():
    tests = [
        # [(from, to), (first, last, count)],
        # gets the whole list because 201704 is before the first 2017 index
        # XXX why is 2013-20 being included ?! 1 year should leave it off
        [(None, '201804'), ('2018-13', '2013-20', 5)],
        [('201801', '201804'), ('2018-13', '2017-51', 4)],  # my_cc_endpoints[4:0:-1]],  # gets 2017-51 but not 2013-20
        [
            ('20180214', '201804'),
            ('2018-13', '2018-05', 3),
        ],  # my_cc_endpoints[4:1:-1]],  # does not get 2017-51, does 2018-05 XXX
        [('20180429', '20180430'), ('2018-13', '2018-13', 1)],  # my_cc_endpoints[4:5]],
        # empty time range
        [('20180430', '20180429'), ('2018-13', '2018-13', 1)],  # my_cc_endpoints[4:5]],
        # very empty time range
        [('20180430', '20100429'), ()],
    ]

    with mock.patch('cdx_toolkit.get_cc_endpoints', return_value=my_cc_endpoints):
        cdx = cdx_toolkit.CDXFetcher(source='cc')
        cdxa = cdx_toolkit.CDXFetcher(source='cc', cc_sort='ascending')
        cdxb = cdx_toolkit.CDXFetcher(source='cc', cc_sort='invalid', loglevel='DEBUG')

        for t in tests:
            params = {
                'from_ts': t[0][0],
                'to': t[0][1],
            }
            cdx_toolkit.commoncrawl.apply_cc_defaults(params)

            with pytest.raises(ValueError):
                index_list = cdxb.customize_index_list(params)

            index_list = cdx.customize_index_list(params)
            index_lista = cdxa.customize_index_list(params)

            if not t[1]:
                assert len(index_list) == 0
                assert len(index_lista) == 0
                continue

            i_from = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][0])
            i_to = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][1])
            i_count = t[1][2]

            assert index_list[0] == i_from, 'test: ' + repr(t)
            assert index_list[-1] == i_to, 'test: ' + repr(t)
            assert len(index_list) == i_count

            assert index_lista[0] == i_to, 'test asc: ' + repr(t)
            assert index_lista[-1] == i_from, 'test asc: ' + repr(t)
            assert len(index_lista) == i_count


def test_customize_index_list_closest():
    tests = [
        [('201801', '20171230', None), ('2018-13', '2017-51', 4)],
        [('201803', '20180214', None), ('2018-13', '2018-05', 3)],
        [('201801', '20171230', '201802'), ('2018-05', '2017-51', 2)],
    ]

    with mock.patch('cdx_toolkit.get_cc_endpoints', return_value=my_cc_endpoints):
        cdx = cdx_toolkit.CDXFetcher(source='cc')

        for t in tests:
            params = {
                'closest': t[0][0],
                'from_ts': t[0][1],
                'to': t[0][2],
            }
            cdx_toolkit.commoncrawl.apply_cc_defaults(params)

            index_list = cdx.customize_index_list(params)

            i_from = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][0])
            i_to = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][1])
            i_count = t[1][2]

            assert index_list[0] == i_from, 'test closest: ' + repr(t)
            assert index_list[-1] == i_to, 'test closest: ' + repr(t)
            assert len(index_list) == i_count


def test_filter_cc_endpoints():
    # gets covered by testing customize_list_index
    pass


@conditional_mock_responses
def test_get_cc_endpoints():
    cc_mirror = 'https://index.commoncrawl.org/'
    cache, filename = cdx_toolkit.commoncrawl.get_cache_names(cc_mirror)
    cache_path = cache + filename

    # make sure cache does not exist
    if os.path.exists(cache_path):
        os.remove(cache_path)

    index_list = cdx_toolkit.commoncrawl.get_cc_endpoints(cc_mirror)

    assert len(index_list) >= 115
