"""Offline unit tests for hf:// bucket URL helpers and the HF reader (no network)."""
import asyncio
import time
from unittest.mock import patch

import pytest

from cdx_toolkit.repackage.hf_utils import (
    HfHttpReader,
    signed_url_expiry,
    is_hf_url,
    hf_fs_path,
    hf_resolve_url,
    retry_after_seconds,
)


def test_is_hf_url():
    assert is_hf_url('hf://buckets/commoncrawl/commoncrawl/crawl-data/x.warc.gz')
    assert not is_hf_url('s3://commoncrawl/crawl-data/x.warc.gz')
    assert not is_hf_url('https://data.commoncrawl.org/x.warc.gz')
    assert not is_hf_url('')
    assert not is_hf_url(None)


def test_hf_fs_path_strips_scheme():
    assert hf_fs_path('hf://buckets/ns/name/a/b.warc.gz') == 'buckets/ns/name/a/b.warc.gz'


def test_hf_fs_path_rejects_non_hf():
    with pytest.raises(ValueError):
        hf_fs_path('s3://bucket/key')


def test_hf_resolve_url_maps_bucket_to_resolve():
    url = 'hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-21/seg/x.warc.gz'
    assert hf_resolve_url(url) == (
        'https://huggingface.co/buckets/commoncrawl/commoncrawl/resolve/'
        'crawl-data/CC-MAIN-2026-21/seg/x.warc.gz'
    )


def test_hf_resolve_url_rejects_non_bucket():
    # only hf://buckets/<ns>/<name>/<key> is supported for resolve mapping
    with pytest.raises(ValueError):
        hf_resolve_url('hf://datasets/foo/bar/x.warc.gz')
    with pytest.raises(ValueError):
        hf_resolve_url('hf://buckets/onlytwo/segments')


def test_retry_after_prefers_retry_after_header():
    assert retry_after_seconds({'Retry-After': '12'}, default=99) == 12.0


def test_retry_after_parses_ratelimit_t_field():
    h = {'RateLimit': '"resolvers";r=0;t=287'}
    assert retry_after_seconds(h, default=99) == 287.0


def test_retry_after_falls_back_to_default():
    assert retry_after_seconds({}, default=7.5) == 7.5
    # malformed Retry-After falls through to default (no RateLimit present)
    assert retry_after_seconds({'Retry-After': 'soon'}, default=3.0) == 3.0


# --- HfHttpReader: resolve each WARC file once, read the records from the signed CDN URL ---

WARC_A = 'hf://buckets/commoncrawl/commoncrawl/crawl-data/a.warc.gz'
WARC_B = 'hf://buckets/commoncrawl/commoncrawl/crawl-data/b.warc.gz'
DATA = bytes(range(256)) * 4


class FakeResponse:
    def __init__(self, status, headers=None, body=b''):
        self.status = status
        self.headers = headers or {}
        self._body = body

    async def read(self):
        return self._body

    async def release(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Resolve URLs redirect to a signed CDN URL; the CDN serves ranges of DATA."""

    def __init__(self, ttl=3600, head_statuses=(), get_statuses=()):
        self.ttl = ttl
        self.head_statuses = list(head_statuses)  # statuses returned before the redirect, e.g. 429
        self.get_statuses = list(get_statuses)  # statuses returned before serving data, e.g. 403
        self.heads, self.gets = [], []

    def head(self, url, headers=None, allow_redirects=True):
        assert not allow_redirects
        self.heads.append((url, headers))
        if self.head_statuses:
            return FakeResponse(self.head_statuses.pop(0), {'Retry-After': '0'})
        expires = int(time.time() + self.ttl)
        location = f'https://cdn.example/{url.rsplit("/", 1)[-1]}?Expires={expires}&n={len(self.heads)}'
        return FakeResponse(302, {'Location': location})

    def get(self, url, headers=None):
        self.gets.append((url, headers))
        if self.get_statuses:
            return FakeResponse(self.get_statuses.pop(0))
        start, end = map(int, headers['Range'][len('bytes='):].split('-'))
        return FakeResponse(206, body=DATA[start:end + 1])


def _read(reader, url, offset, length):
    return reader.ranged_get(url, offset, length, max_attempts=3, base_backoff_seconds=0)


def test_signed_url_expiry():
    assert signed_url_expiry('https://cdn.example/x?Expires=1791445181&Signature=s', 600) == 1791445181
    assert abs(signed_url_expiry('https://cdn.example/x', 600) - (time.time() + 600)) < 5


def test_resolves_each_file_once():
    session = FakeSession()
    reader = HfHttpReader(session, token='tok')

    async def run():
        return [await _read(reader, url, o, 10) for url, o in [(WARC_A, 0), (WARC_A, 100), (WARC_B, 5), (WARC_A, 50)]]

    assert asyncio.run(run()) == [DATA[0:10], DATA[100:110], DATA[5:15], DATA[50:60]]
    assert [url for url, _ in session.heads] == [
        'https://huggingface.co/buckets/commoncrawl/commoncrawl/resolve/crawl-data/a.warc.gz',
        'https://huggingface.co/buckets/commoncrawl/commoncrawl/resolve/crawl-data/b.warc.gz',
    ]
    assert session.heads[0][1] == {'Authorization': 'Bearer tok'}
    # the token is not sent to the CDN, the URL is signed
    assert all(url.startswith('https://cdn.example/') and 'Authorization' not in h for url, h in session.gets)
    assert (reader.resolve_count, reader.read_count) == (2, 4)


def test_concurrent_reads_share_one_resolve():
    session = FakeSession()
    reader = HfHttpReader(session)

    async def run():
        return await asyncio.gather(*[_read(reader, WARC_A, o, 4) for o in range(0, 80, 8)])

    assert asyncio.run(run()) == [DATA[o:o + 4] for o in range(0, 80, 8)]
    assert len(session.heads) == 1


def test_resolves_again_before_expiry():
    session = FakeSession(ttl=HfHttpReader.EXPIRY_MARGIN - 1)  # signed URLs that are about to expire
    reader = HfHttpReader(session)

    async def run():
        await _read(reader, WARC_A, 0, 4)
        await _read(reader, WARC_A, 4, 4)

    asyncio.run(run())
    assert len(session.heads) == 2


def test_resolves_again_when_cdn_rejects_signed_url():
    session = FakeSession(get_statuses=[403])
    reader = HfHttpReader(session)

    assert asyncio.run(_read(reader, WARC_A, 10, 4)) == DATA[10:14]
    assert len(session.heads) == 2  # the rejected URL was resolved again
    assert session.gets[-1][0].endswith('&n=2')


def test_waits_on_resolver_rate_limit():
    session = FakeSession(head_statuses=[429, 429])
    reader = HfHttpReader(session)

    async def no_sleep(_):
        return None

    with patch('cdx_toolkit.repackage.hf_utils.asyncio.sleep', side_effect=no_sleep):
        assert asyncio.run(_read(reader, WARC_A, 0, 4)) == DATA[0:4]

    assert reader.throttle_count == 2
    assert reader.resolve_count == 1
