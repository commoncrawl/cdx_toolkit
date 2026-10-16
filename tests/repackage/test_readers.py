import asyncio
import contextlib
from unittest.mock import patch

import pytest

from cdx_toolkit.repackage.jobs import RangeJob, WaybackJob
from cdx_toolkit.repackage.readers import Readers
from cdx_toolkit.repackage.readers.http import HttpRangeReader, RangeError, check_range_response
from cdx_toolkit.repackage.readers.wayback import WaybackReader

from tests.conftest import requires_aioboto3


@requires_aioboto3
def test_readers_by_job():
    from cdx_toolkit.repackage.readers.s3 import S3RangeReader

    hf_reader = object()

    @contextlib.asynccontextmanager
    async def fake_make_hf_reader(max_attempts, base_backoff_seconds):
        assert (max_attempts, base_backoff_seconds) == (3, 0.1)
        yield hf_reader

    async def main():
        async with contextlib.AsyncExitStack() as stack:
            readers = Readers(stack, lambda: None, max_attempts=3, base_backoff_seconds=0.1)

            def for_url(url):
                return readers.for_job(RangeJob(url, 0, 1))

            http = await for_url('https://data.commoncrawl.org/a.warc.gz')
            s3 = await for_url('s3://commoncrawl/a.warc.gz')
            hf = await for_url('hf://buckets/commoncrawl/commoncrawl/a.warc.gz')
            # one reader per scheme
            assert await for_url('s3://commoncrawl/b.warc.gz') is s3
            assert await for_url('http://example.org/b.warc.gz') is http
            # a wayback replay, by the type of the job (its replay URL is an http URL)
            wb = 'https://web.archive.org/web'
            wayback = await readers.for_job(WaybackJob(wb + '/1id_/http://example.com/', {}, wb))
            return http, s3, hf, wayback

    with patch('aioboto3.Session'), patch('cdx_toolkit.repackage.readers.hf.make_hf_reader', fake_make_hf_reader):
        http, s3, hf, wayback = asyncio.run(main())
    assert isinstance(http, HttpRangeReader)
    assert isinstance(wayback, WaybackReader)
    assert isinstance(s3, S3RangeReader) and (s3.max_attempts, s3.base_backoff_seconds) == (3, 0.1)
    assert hf is hf_reader


@pytest.mark.parametrize('status, content_range, body_length, offset, valid', [
    (206, 'bytes 10-19/100', 10, 10, True),
    (206, 'bytes 10-19/*', 10, 10, True),
    (200, None, 10, 0, True),  # the whole file is the requested range
    (200, None, 100, 10, False),  # range ignored
    (206, 'bytes 11-20/100', 10, 10, False),  # wrong range
    (206, 'bytes 10-19/100', 9, 10, False),  # truncated body
    (206, None, 10, 10, False),
    (302, None, 0, 10, False),  # redirects are not followed
])
def test_check_range_response(status, content_range, body_length, offset, valid):
    def check():
        check_range_response(status, content_range, body_length, offset, 10, 'https://example.org/x')

    if valid:
        check()
    else:
        with pytest.raises(RangeError):
            check()
