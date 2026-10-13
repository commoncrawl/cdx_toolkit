"""Hugging Face Storage Bucket support for repackage reads/writes.

HF Storage Buckets (``hf://buckets/<namespace>/<name>/<key>``) expose data only
through the Hugging Face filesystem / HTTP resolve API -- there is no S3 API yet.
So cdxt cannot reach a bucket via the aioboto3 read path. Instead WARC byte ranges
are fetched with async ``aiohttp`` ranged ``GET`` requests against the CDN: each WARC
file is resolved once (``HEAD https://huggingface.co/buckets/<ns>/<name>/resolve/<key>``
-> redirect to a signed CDN URL, valid for about an hour), and all records of the file
are then read from the signed URL. Every ``/resolve/`` request counts against the Hub's
"Resolvers" rate limit (e.g. 5,000 per 5 minutes for free users, see
https://huggingface.co/docs/hub/en/rate-limits), the CDN requests do not.

The records are byte-identical to the same records read from ``s3://commoncrawl``.
(Reading with ``HfFileSystem.cat_file`` instead was ~10x slower, used ~12x more memory
and dropped records under load, see the cdxt-benchmark read-backend study.)
"""
import asyncio
import collections
import contextlib
import logging
import re
import time
from typing import Tuple
from urllib.parse import parse_qs, urlparse

from cdx_toolkit.repackage.readers.http import check_range_response
from cdx_toolkit.repackage.urls import HF_SCHEME, is_hf_url

logger = logging.getLogger(__name__)

HF_ENDPOINT = 'https://huggingface.co'


def hf_fs_path(url: str) -> str:
    """``hf://buckets/ns/name/key`` -> ``buckets/ns/name/key`` (HfFileSystem path)."""
    if not is_hf_url(url):
        raise ValueError(f'Not an hf:// URL: {url}')
    return url[len(HF_SCHEME):]


def hf_resolve_url(url: str) -> str:
    """Map an ``hf://`` bucket URL to its CDN-fronted HTTP(S) resolve URL.

    ``hf://buckets/<ns>/<name>/<key>``
        -> ``https://huggingface.co/buckets/<ns>/<name>/resolve/<key>``
    """
    path = hf_fs_path(url)
    parts = path.split('/', 3)
    if len(parts) < 4 or parts[0] != 'buckets':
        raise ValueError(
            f'Unsupported hf:// URL (expected hf://buckets/<ns>/<name>/<key>): {url}'
        )
    repo = '/'.join(parts[:3])  # buckets/<ns>/<name>
    key = parts[3]
    return f'{HF_ENDPOINT}/{repo}/resolve/{key}'


def retry_after_seconds(headers, default: float) -> float:
    """Seconds to wait after a 429, from the response headers.

    Prefers ``Retry-After`` (delta-seconds); else the ``t=`` field of the
    standardized ``RateLimit`` header (seconds until the window resets); else
    ``default``.
    """
    ra = headers.get('Retry-After')
    if ra:
        try:
            return max(0.0, float(ra))
        except (TypeError, ValueError):
            pass
    m = re.search(r'(?:^|[;,\s])t\s*=\s*(\d+)', headers.get('RateLimit', '') or '')
    if m:
        return float(m.group(1))
    return default


def signed_url_expiry(url: str, default_ttl: float) -> float:
    """Expiry (epoch seconds) of a signed CDN URL from its ``Expires`` parameter."""
    expires = parse_qs(urlparse(url).query).get('Expires', [None])[0]
    if expires and expires.isdigit():
        return float(expires)
    return time.time() + default_ttl


class HfHttpReader:
    """Async ranged ``GET`` requests against the HF CDN (aiohttp), resolving each WARC file once.

    The ``/resolve/`` URL of a WARC file is requested once (``HEAD``, no redirect
    followed) to get the signed CDN URL it redirects to; that URL is cached (LRU) and
    all records of the file are fetched from it with ``Range`` requests. The URL is
    resolved again shortly before it expires, or when the CDN rejects it (401/403/410).

    Only the resolve requests count against the HF Resolvers rate limit. On ``429`` the
    reader honours the server-provided wait (``Retry-After`` / ``RateLimit`` header)
    instead of failing, and tallies the throttle events/wait so a run reports whether it
    was rate-limited.
    """

    # Bound the wait-out so a job can ride out a 5-minute window but not hang
    # forever if the limit never clears.
    MAX_THROTTLE_WAIT = 360.0  # cap a single 429 backoff (seconds)
    MAX_THROTTLE_RETRIES = 60  # consecutive 429s before giving up on a record

    # Resolve again this many seconds before the signed URL expires.
    EXPIRY_MARGIN = 300.0
    # Lifetime assumed for a signed URL without an ``Expires`` parameter.
    DEFAULT_TTL = 600.0
    # Number of signed URLs kept; the input is sorted by WARC file, so few are in use at a time.
    MAX_CACHED_URLS = 4096

    def __init__(self, session, token=None):
        self.session = session
        self.token = token
        self.throttle_count = 0       # number of 429s seen
        self.throttle_wait_s = 0.0    # cumulative time spent backing off 429s
        self.resolve_count = 0        # number of /resolve/ requests
        self.read_count = 0           # number of records read
        self._policy_logged = False
        # resolve URL -> (signed CDN URL, expiry); a lock per resolve URL so concurrent
        # readers of the same file share one resolve request
        self._signed_urls: 'collections.OrderedDict[str, Tuple[str, float]]' = collections.OrderedDict()
        self._locks = {}

    def _auth_headers(self) -> dict:
        return {'Authorization': f'Bearer {self.token}'} if self.token else {}

    async def _wait_for_rate_limit(self, resp, throttles: int, base_backoff_seconds: float, url: str) -> None:
        self.throttle_count += 1
        default = min(30.0, base_backoff_seconds * 2 ** min(throttles, 6))
        wait = retry_after_seconds(resp.headers, default)
        if not self._policy_logged:
            self._policy_logged = True
            logger.warning(
                'HF resolver rate limit hit (429); RateLimit=%r Policy=%r. '
                'Throughput is now capped by the Resolvers quota.',
                resp.headers.get('RateLimit'),
                resp.headers.get('RateLimit-Policy'),
            )
        if throttles > self.MAX_THROTTLE_RETRIES or wait > self.MAX_THROTTLE_WAIT:
            raise OSError(f'HF 429: gave up after {throttles} throttle waits (last wait {wait:.0f}s) for {url}')
        self.throttle_wait_s += wait
        await asyncio.sleep(wait)

    async def _resolve(self, resolve: str, *, max_attempts: int, base_backoff_seconds: float) -> Tuple[str, float]:
        """Resolve a file to its signed CDN URL and the URL's expiry."""
        attempt = 0
        throttles = 0
        while True:
            try:
                async with self.session.head(resolve, headers=self._auth_headers(), allow_redirects=False) as resp:
                    if resp.status == 429:
                        throttles += 1
                        await self._wait_for_rate_limit(resp, throttles, base_backoff_seconds, resolve)
                        continue  # does not consume the hard-error attempt budget
                    self.resolve_count += 1
                    location = resp.headers.get('Location')
                    if resp.status in (301, 302, 303, 307, 308) and location:
                        return location, signed_url_expiry(location, self.DEFAULT_TTL)
                    if resp.status == 200:
                        # served without a redirect: read from the resolve URL itself
                        return resolve, time.time() + self.DEFAULT_TTL
                    raise OSError(f'HF HEAD {resolve} -> {resp.status}')
            except Exception:  # noqa: BLE001 - retry any transient resolve error
                attempt += 1
                if attempt >= max_attempts:
                    raise
                await asyncio.sleep(min(8.0, base_backoff_seconds * 2 ** (attempt - 1)))

    async def _signed_url(self, resolve: str, *, refresh: bool, **kwargs) -> str:
        lock = self._locks.setdefault(resolve, asyncio.Lock())
        async with lock:
            cached = self._signed_urls.get(resolve)
            if cached and not refresh and cached[1] - time.time() > self.EXPIRY_MARGIN:
                self._signed_urls.move_to_end(resolve)
                return cached[0]
            url, expiry = await self._resolve(resolve, **kwargs)
            self._signed_urls[resolve] = (url, expiry)
            self._signed_urls.move_to_end(resolve)
            while len(self._signed_urls) > self.MAX_CACHED_URLS:
                old, _ = self._signed_urls.popitem(last=False)
                self._locks.pop(old, None)
            return url

    async def ranged_get(
        self,
        url: str,
        offset: int,
        length: int,
        *,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
    ) -> bytes:
        resolve = hf_resolve_url(url)
        end = offset + length - 1  # inclusive HTTP Range
        attempt = 0
        throttles = 0
        refresh = False
        while True:
            try:
                signed_url = await self._signed_url(
                    resolve, refresh=refresh, max_attempts=max_attempts, base_backoff_seconds=base_backoff_seconds
                )
                refresh = False
                headers = {'Range': f'bytes={offset}-{end}'}
                if signed_url.startswith(HF_ENDPOINT):
                    headers.update(self._auth_headers())
                async with self.session.get(signed_url, headers=headers) as resp:
                    if resp.status == 429:
                        throttles += 1
                        await self._wait_for_rate_limit(resp, throttles, base_backoff_seconds, signed_url)
                        continue  # does not consume the hard-error attempt budget
                    if resp.status in (401, 403, 410):
                        # signed URL expired or revoked: resolve the file again
                        await resp.release()
                        refresh = True
                        raise OSError(f'HF CDN GET -> {resp.status}, resolving {resolve} again')
                    if resp.status not in (200, 206):
                        body = await resp.read()
                        raise OSError(f'HF GET {resolve} -> {resp.status}: {body[:200]!r}')
                    data = await resp.read()
                    check_range_response(
                        resp.status, resp.headers.get('Content-Range'), len(data), offset, length, resolve
                    )
                self.read_count += 1
                return data
            except Exception:  # noqa: BLE001 - retry any transient read error
                attempt += 1
                if attempt >= max_attempts:
                    raise
                await asyncio.sleep(min(8.0, base_backoff_seconds * 2 ** (attempt - 1)))

    async def aclose(self):
        logger.info('HF reader: %d records read with %d resolve requests', self.read_count, self.resolve_count)
        if self.throttle_count:
            logger.warning(
                'HF resolver throttling: %d x 429, %.1fs total spent backing off '
                '(read throughput was rate-limit bound)',
                self.throttle_count,
                self.throttle_wait_s,
            )


@contextlib.asynccontextmanager
async def make_hf_reader():
    """Async context manager yielding an HF reader that owns an ``aiohttp`` session.

    Imports are lazy so the optional ``hf`` extra is only required when actually
    reading from a bucket.
    """
    import aiohttp
    from huggingface_hub import get_token

    timeout = aiohttp.ClientTimeout(total=300, sock_connect=15)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        reader = HfHttpReader(session, token=get_token())
        try:
            yield reader
        finally:
            await reader.aclose()
