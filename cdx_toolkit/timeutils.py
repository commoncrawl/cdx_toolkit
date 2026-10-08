import calendar
import datetime
from email.utils import parsedate
import logging

LOGGER = logging.getLogger(__name__)

# confusingly, python's documentation refers to their float version
# of the unix time as a 'timestamp'. This code uses 'timestamp' to
# mean the CDX concept of timestamp.

TIMESTAMP = '%Y%m%d%H%M%S'
TIMESTAMP_LOW = '19780101000000'
TIMESTAMP_HIGH = '29991231235959'


def pad_timestamp(ts):
    '''
    >>> pad_timestamp('1998')
    '19980101000000'
    '''
    return ts + TIMESTAMP_LOW[len(ts):]


def pad_timestamp_up(ts):
    '''
    >>> pad_timestamp_up('199802')
    '19980228235959'
    >>> pad_timestamp_up('200002')
    '20000229235959'
    >>> pad_timestamp_up('20260410')
    '20260410235959'
    '''
    padded = ts + TIMESTAMP_HIGH[len(ts):]
    if len(ts) < 8:  # no day given: the last day of the month
        last_day = calendar.monthrange(int(padded[:4]), int(padded[4:6]))[1]
        padded = padded[:6] + f'{last_day:02d}' + padded[8:]
    return padded


def timestamp_to_time(ts):
    '''
    >>> timestamp_to_time('1999')
    915148800.0
    '''
    utc = datetime.timezone.utc
    padded = pad_timestamp(ts)
    try:
        return datetime.datetime.strptime(padded, TIMESTAMP).replace(tzinfo=utc).timestamp()
    except ValueError:
        # users may try to put a Unixtime in
        # the web was born: 19890312 == 605664000
        if ts.isdigit() and int(ts) > 605664000 and int(ts) < 1989031200:
            LOGGER.error('hint: unixtime {} is cdx timestamp {}'.format(ts, time_to_timestamp(int(ts))))
            raise ValueError('cannot parse timestamp, cdx timestamps are not unix timestamps: '+ts) from None
        else:
            raise ValueError('cannot parse timestamp, is it a valid cdx timestamp?: '+ts) from None


def time_to_timestamp(t):
    '''
    >>> time_to_timestamp(915148800.0)
    '19990101000000'
    '''
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc).strftime(TIMESTAMP)


CC_TIMESTAMP = '%Y-%W-%w'


def cc_index_to_time(cc):
    '''
    Convert a Commoncrawl index name YYYY-isoweek to a unixtime

    >>> cc_index_to_time('2018-02')
    1515888000.0
    '''
    utc = datetime.timezone.utc
    return datetime.datetime.strptime(cc+'-0', CC_TIMESTAMP).replace(tzinfo=utc).timestamp()


def cc_index_to_time_special(cc):
    '''
    Convert a "special" Commoncrawl index name to a unixtime

    >>> cc_index_to_time_special('2012')
    1338508800.0
    >>> cc_index_to_time_special('2009-2010')
    1283299200.0
    '''

    table = {  # times provided by Sebastian
        '2012': timestamp_to_time('201206'),  # end 20120605, start was 20120127
        '2009-2010': timestamp_to_time('201009'),  # end 20100910, start was 20100910
        '2008-2009': timestamp_to_time('200901'),  # end 20090109, start was 20080509
    }
    if cc in table:
        return table[cc]

    LOGGER.error('could not convert endpoint name %s to an end time', cc)


def validate_timestamps(params):
    # will have to change once we start supporting sub-second timestamps
    for key in ('from_ts', 'to', 'closest'):
        if key in params:
            value = params[key]
            if isinstance(value, str):
                if not value.isdigit():
                    raise ValueError('invalid parameter {} {!r}'.format(key, value))
            elif isinstance(value, int):
                pass
            else:
                raise ValueError('invalid parameter {} {!r}'.format(key, value))


'''
Code cribbed from warcio/timeutls.py. Not calling it directly because we hope
to make warcio optional someday.
'''

ISO_DT = '%Y-%m-%dT%H:%M:%SZ'


def http_date_to_datetime(string):
    """
    >>> http_date_to_datetime('Thu, 26 Dec 2013 09:50:10 GMT')
    datetime.datetime(2013, 12, 26, 9, 50, 10)
    """
    return datetime.datetime(*parsedate(string)[:6])


def datetime_to_iso_date(the_datetime):
    """
    >>> datetime_to_iso_date(datetime.datetime(2013, 12, 26, 10, 11, 12))
    '2013-12-26T10:11:12Z'

    >>> datetime_to_iso_date( datetime.datetime(2013, 12, 26, 10, 11, 12))
    '2013-12-26T10:11:12Z'
    """

    return the_datetime.strftime(ISO_DT)
