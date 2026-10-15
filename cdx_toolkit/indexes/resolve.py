from typing import Optional
from urllib.parse import urlparse

from .base import IndexBackend, UsageError
from .backends.cdx_api import CdxApiBackend


# index names that map to CDX API sources known by CDXFetcher
INDEX_NAMES = ('cc', 'ia')

# index names that map to locations of Common Crawl's columnar URL index (queried with DuckDB)
INDEX_ALIASES = {
    'cc-url-index-s3': 's3://commoncrawl/cc-index/table/cc-main/warc/',
    'cc-url-index-hf': 'hf://buckets/commoncrawl/commoncrawl/cc-index/table/cc-main/warc/',
}

INDEX_BACKENDS = ('auto', 'cdx-api', 'cdx-files', 'duckdb', 'athena')

GLOB_CHARS = ('*', '?', '[')


def is_cdx_api_location(location: str) -> bool:
    """An index name, or an http(s) URL whose last path segment has no file extension."""
    if location in INDEX_NAMES:
        return True
    parsed = urlparse(location)
    if parsed.scheme not in ('http', 'https'):
        return False
    if any(c in parsed.path for c in GLOB_CHARS):
        return False
    last_segment = parsed.path.rstrip('/').rsplit('/', 1)[-1]
    return '.' not in last_segment


def detect_index_backend(location: str) -> str:
    """The backend of an index, from its location only (no network access)."""
    if location in INDEX_ALIASES or location.endswith('.parquet'):
        return 'duckdb'
    if location.startswith('athena://'):
        return 'athena'
    if is_cdx_api_location(location):
        return 'cdx-api'
    return 'cdx-files'


def open_index(
    location: str,
    backend: str = 'auto',
    cc_mirror: Optional[str] = None,
    cc_sort: Optional[str] = None,
    assume_yes: bool = False,
) -> IndexBackend:
    """Map an index name or location to the backend that can query it.

    - 'cc', 'ia', or http(s) URLs without a file extension -> CDX API
    - 'athena://...' -> Athena
    - 'cc-url-index-s3', 'cc-url-index-hf', '*.parquet', or any path with backend 'duckdb' -> DuckDB
    - anything else (local or remote CDX files, globs) -> CDX files

    `backend` ('cdx-api', 'cdx-files', 'duckdb' or 'athena') overrides the detection, e.g. 'cdx-files' for a CDX
    file at an http(s) URL without file extension.
    """
    if backend not in INDEX_BACKENDS:
        raise UsageError(f'unknown index backend {backend!r} (choose from {", ".join(INDEX_BACKENDS)})')
    if backend == 'auto':
        backend = detect_index_backend(location)
    location = INDEX_ALIASES.get(location, location)

    if backend == 'athena':
        from .backends.athena import AthenaBackend

        return AthenaBackend(location, assume_yes=assume_yes)

    if backend == 'duckdb':
        if location in INDEX_NAMES or is_cdx_api_location(location) or location.startswith('athena://'):
            raise UsageError(f'{location} cannot be queried with DuckDB; use a path to the parquet index')
        from .backends.duckdb import DuckDbBackend

        return DuckDbBackend(location, assume_yes=assume_yes)

    if backend == 'cdx-api':
        if location not in INDEX_NAMES and urlparse(location).scheme not in ('http', 'https'):
            raise UsageError(f'{location} is not a CDX API; use cc, ia or the http(s) URL of a CDX API server')
        return CdxApiBackend(location, cc_mirror=cc_mirror, cc_sort=cc_sort)

    if location in INDEX_NAMES or location.startswith('athena://'):
        raise UsageError(f'{location} is not a path of CDX files')
    from .backends.cdx_files import CdxFilesBackend

    return CdxFilesBackend(location)
