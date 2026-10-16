import pytest

import cdx_toolkit.timeutils as timeutils


def test_padding():
    assert timeutils.pad_timestamp('1998') == '19980101000000'
    assert timeutils.pad_timestamp_up('199802') == '19980228235959'
    assert timeutils.pad_timestamp_up('199812') == '19981231235959'
    assert timeutils.pad_timestamp_up('202402') == '20240229235959'  # leap year
    assert timeutils.pad_timestamp_up('20260410') == '20260410235959'  # the day is kept
    assert timeutils.pad_timestamp_up('2026041012') == '20260410125959'


def test_time_timestamp():
    tests = (
        ('1978', 252460800.0),  # tests our timezone
        ('1999', 915148800.0),
        ('19981231235959', 915148799.0),  # previous line minus 1s
    )
    for ts, t in tests:
        assert timeutils.timestamp_to_time(ts) == t
        assert timeutils.time_to_timestamp(t) == timeutils.pad_timestamp(ts)

    with pytest.raises(ValueError):
        timeutils.timestamp_to_time('19990231')  # invalid day of month

    with pytest.raises(ValueError, match='are not unix timestamps'):
        timeutils.timestamp_to_time('1598411009')

    with pytest.raises(ValueError, match='is it a valid cdx timestamp'):
        timeutils.timestamp_to_time('x')


def test_validate_timestamps():
    with pytest.raises(ValueError):
        timeutils.validate_timestamps({'to': 'asdf'})
    with pytest.raises(ValueError):
        timeutils.validate_timestamps({'to': {}})
    timeutils.validate_timestamps({'to': '12345'})
    timeutils.validate_timestamps({'to': 12345})
    assert True
