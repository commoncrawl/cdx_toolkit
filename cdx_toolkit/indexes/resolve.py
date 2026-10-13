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

INDEX_FORMATS = ('auto', 'cdx', 'athena', 'duckdb')

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


def detect_index_format(location: str) -> str:
    """Index format from the location only (no network access)."""
    if location in INDEX_ALIASES or location.endswith('.parquet'):
        return 'duckdb'
    if location.startswith('athena://'):
        return 'athena'
    return 'cdx'


def resolve_index(
    location: str,
    index_format: str = 'auto',
    cc_mirror: Optional[str] = None,
    cc_sort: Optional[str] = None,
    assume_yes: bool = False,
) -> IndexBackend:
    """Map an index name or location to the backend that can query it.

    - 'cc', 'ia', or http(s) URLs without a file extension -> CDX API
    - 'athena://...' -> Athena
    - 'cc-url-index-s3', 'cc-url-index-hf', '*.parquet', or any path with --index-format duckdb -> DuckDB
    - anything else (local or remote CDX files, globs) -> CDX files
    """
    if index_format not in INDEX_FORMATS:
        raise UsageError(f'unknown index format {index_format!r} (choose from {", ".join(INDEX_FORMATS)})')
    if index_format == 'auto':
        index_format = detect_index_format(location)
    location = INDEX_ALIASES.get(location, location)

    if index_format == 'athena':
        from .backends.athena import AthenaBackend

        return AthenaBackend(location, assume_yes=assume_yes)

    if index_format == 'duckdb':
        if location in INDEX_NAMES or is_cdx_api_location(location) or location.startswith('athena://'):
            raise UsageError(f'{location} cannot be queried with DuckDB; use a path to the parquet index')
        from .backends.duckdb import DuckDbBackend

        return DuckDbBackend(location, assume_yes=assume_yes)

    if is_cdx_api_location(location):
        return CdxApiBackend(location, cc_mirror=cc_mirror, cc_sort=cc_sort)

    from .backends.cdx_files import CdxFilesBackend

    return CdxFilesBackend(location)
