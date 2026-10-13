import logging
from typing import Iterator, Mapping, Optional

import cdx_toolkit
from cdx_toolkit.commoncrawl import normalize_crawl

from ..base import IndexBackend, UsageError
from ..query import IndexQuery


LOGGER = logging.getLogger(__name__)


class CdxApiBackend(IndexBackend):
    """A CDX API server: Common Crawl ('cc'), the Internet Archive ('ia'), or any pywb/wayback URL."""

    name = 'CDX API'
    supported_options = frozenset({
        'urls', 'hostnames', 'domains', 'crawl', 'from_ts', 'to', 'closest', 'filters',
        'url_fgrep', 'url_fgrepv', 'limit',
    })

    def __init__(self, source: str, cc_mirror: Optional[str] = None, cc_sort: Optional[str] = None):
        self.source = source
        self.cc_mirror = cc_mirror
        self.cc_sort = cc_sort

    def validate(self, query: IndexQuery) -> None:
        super().validate(query)
        if not query.url_patterns():
            raise UsageError('the CDX API needs at least one --url, --hostname or --domain')
        if query.crawl and self.source != 'cc':
            raise UsageError('--crawl can only be used with the Common Crawl index (cc)')

    def iter_records(self, query: IndexQuery) -> Iterator[Mapping]:
        """The captures of all selectors; a capture selected by more than one is only returned once.

        With several selectors, --limit is applied here (after removing the duplicates), not by the server."""
        cdx = self._fetcher(query)
        patterns = query.url_patterns()
        remaining = query.limit
        seen = set() if len(patterns) > 1 else None
        for url, match_type in patterns:
            params = self._params(query, match_type, limit=remaining if seen is None else None)
            # closest only makes sense for a single (non-paged) query
            captures = cdx.get(url, **params) if query.closest else cdx.iter(url, **params)
            for capture in captures:
                if seen is not None:
                    identity = capture_identity(capture)
                    if identity in seen:
                        continue
                    seen.add(identity)
                yield capture
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

    def count(self, query: IndexQuery) -> int:
        """Estimate from the number of index pages, at most --limit; overlapping selectors are counted more
        than once."""
        cdx = self._fetcher(query)
        count = sum(
            cdx.get_size_estimate(url, **self._params(query, match_type, limit=query.limit))
            for url, match_type in query.url_patterns()
        )
        return min(count, query.limit) if query.limit else count

    def explain(self, query: IndexQuery) -> str:
        lines = []
        for url, match_type in query.url_patterns():
            params = ' '.join(f'{k}={v}' for k, v in self._params(query, match_type, limit=query.limit).items())
            lines.append(f'{self.source}: url={url} {params}'.rstrip())
        if query.crawl:
            lines.append(f'crawls: {query.crawl}')
        return '\n'.join(lines) + '\n'

    def _fetcher(self, query: IndexQuery) -> cdx_toolkit.CDXFetcher:
        kwargs = {'source': self.source}
        if self.cc_mirror:
            kwargs['cc_mirror'] = self.cc_mirror
        if self.cc_sort:
            kwargs['cc_sort'] = self.cc_sort
        if query.crawl:
            kwargs['crawl'] = normalize_crawl([query.crawl])
        return cdx_toolkit.CDXFetcher(**kwargs)

    @staticmethod
    def _params(query: IndexQuery, match_type: Optional[str], limit: Optional[int]) -> dict:
        params = {}
        if limit:
            params['limit'] = limit
        if query.from_ts:
            params['from_ts'] = query.from_ts
        if query.to:
            params['to'] = query.to
        if query.closest:
            params['closest'] = query.closest
        if query.filters:
            params['filter'] = list(query.filters)
        if match_type:
            params['matchType'] = match_type
        return params


def capture_identity(capture: Mapping) -> tuple:
    """What distinguishes captures: the same URL can be captured at the same time more than once (e.g. in
    different crawls), so the WARC location is part of the identity."""
    return tuple(capture.get(field) for field in ('urlkey', 'url', 'timestamp', 'filename', 'offset', 'digest'))
