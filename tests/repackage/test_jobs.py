import pytest

from cdx_toolkit.repackage.jobs import InvalidWaybackCapture, WaybackSource, join_warc_url

IA_CAPTURE = {'url': 'http://example.com/a b', 'timestamp': '20240101120000', 'status': '200'}


def test_join_warc_url_relative():
    assert (
        join_warc_url('https://data.commoncrawl.org', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )


def test_join_warc_url_no_double_slash():
    assert (
        join_warc_url('https://data.commoncrawl.org/', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )
    assert join_warc_url('s3://bucket', '/crawl-data/x.warc.gz') == 's3://bucket/crawl-data/x.warc.gz'


def test_join_warc_url_empty_prefix():
    assert join_warc_url('', 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'
    assert join_warc_url(None, 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'


def test_join_warc_url_absolute_filename_passthrough():
    assert join_warc_url('https://data.commoncrawl.org', 's3://cc/x.warc.gz') == 's3://cc/x.warc.gz'
    assert join_warc_url('', 'https://host/x.warc.gz') == 'https://host/x.warc.gz'


def wayback_job(record, wb='ia'):
    return next(WaybackSource([record], wb).iter_range_jobs())


def test_wayback_job():
    job = wayback_job(dict(IA_CAPTURE, status=200, mime='text/html'))
    assert job.url == 'https://web.archive.org/web/20240101120000id_/http%3A//example.com/a%20b'
    assert job.wb == 'https://web.archive.org/web'
    assert job.capture == {**IA_CAPTURE, 'mime': 'text/html'}  # the status as a string, like the CDX API's
    assert wayback_job(IA_CAPTURE, 'https://wb.example.org/web').url.startswith('https://wb.example.org/web/')


def test_wayback_jobs_from_another_wayback_with_warc_location():
    """pywb servers return filename/offset/length too, relative to their own storage: replayed, not copied."""
    capture = dict(IA_CAPTURE, filename='collections/my/archive/x.warc.gz', offset='0', length='10')
    assert wayback_job(capture, 'https://wb.example.org/web').capture == capture


@pytest.mark.parametrize('filename', [
    'crawl-data/CC-MAIN-2024-33/segments/1/warc/x.warc.gz',
    'https://data.commoncrawl.org/crawl-data/CC-NEWS/2024/01/x.warc.gz',
    's3://commoncrawl/crawl-data/CC-MAIN-2024-33/x.warc.gz',
])
def test_wayback_rejects_common_crawl_captures(filename):
    with pytest.raises(InvalidWaybackCapture, match='Common Crawl capture .* do not use --wb'):
        wayback_job(dict(IA_CAPTURE, filename=filename, offset='0', length='10'))


@pytest.mark.parametrize('change, error', [
    ({'url': None}, 'has no url'),
    ({'timestamp': ''}, 'has no timestamp'),
    ({'status': None}, 'has no status'),
    ({'url': 'example.com/a'}, 'not an http.s. URL'),
    ({'url': 'ftp://example.com/a'}, 'not an http.s. URL'),
    ({'url': 'http://'}, 'not an http.s. URL'),
    ({'timestamp': '2024-01-01'}, 'invalid timestamp'),
    ({'timestamp': '202'}, 'invalid timestamp'),
])
def test_wayback_rejects_invalid_captures(change, error):
    with pytest.raises(InvalidWaybackCapture, match=error):
        wayback_job({**IA_CAPTURE, **change})
