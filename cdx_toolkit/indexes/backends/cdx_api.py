from dataclasses import dataclass
import logging
from typing import Iterator, List, Mapping, Optional

import cdx_toolkit
from cdx_toolkit.commoncrawl import normalize_crawl
from cdx_toolkit.compat import munge_filter

from ..base import IndexBackend, QueryPlan, UsageError
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
        for expression in query.filters:
            # the check of CDXFetcher, which would only run when the captures are read
            try:
                munge_filter([expression], self.source)
            except ValueError as e:
                raise UsageError(
                    f'--filter {expression!r}: {e} (the Internet Archive supports only FIELD:REGEX and '
                    '!FIELD:REGEX, e.g. status:200)'
                ) from None

    def plan(self, query: IndexQuery) -> 'ApiPlan':
        requests = [ApiRequest(url, api_params(query, match_type)) for url, match_type in query.url_patterns()]
        return ApiPlan(self.source, requests, query.limit, query.crawl)

    def select(self, query: IndexQuery) -> Iterator[Mapping]:
        """The captures of all selectors; a capture selected by more than one is only returned once."""
        plan = self.plan(query)
        cdx = self._fetcher(query)
        seen = set() if len(plan.requests) > 1 else None
        for request in plan.requests:
            params = request.params_with(plan.server_limit)
            # closest only makes sense for a single (non-paged) query
            captures = cdx.get(request.url, **params) if query.closest else cdx.iter(request.url, **params)
            for capture in captures:
                if seen is not None:
                    identity = capture_identity(capture)
                    if identity in seen:
                        continue
                    seen.add(identity)
                yield capture

    def select_count(self, query: IndexQuery) -> int:
        """Estimate from the number of index pages, at most --limit; overlapping selectors are counted more
        than once."""
        cdx = self._fetcher(query)
        count = sum(
            cdx.get_size_estimate(request.url, **request.params_with(query.limit))
            for request in self.plan(query).requests
        )
        return min(count, query.limit) if query.limit else count

    def _fetcher(self, query: IndexQuery) -> cdx_toolkit.CDXFetcher:
        kwargs = {'source': self.source}
        if self.cc_mirror:
            kwargs['cc_mirror'] = self.cc_mirror
        if self.cc_sort:
            kwargs['cc_sort'] = self.cc_sort
        if query.crawl:
            kwargs['crawl'] = normalize_crawl([query.crawl])
        return cdx_toolkit.CDXFetcher(**kwargs)


@dataclass
class ApiRequest:
    """One request (series of paged requests) to the CDX API: a URL and its parameters (without the limit)."""

    url: str
    params: dict

    def params_with(self, limit: Optional[int]) -> dict:
        return {'limit': limit, **self.params} if limit else dict(self.params)


@dataclass
class ApiPlan(QueryPlan):
    """One request per URL selector (--url, --hostname, --domain)."""

    source: str
    requests: List[ApiRequest]
    limit: Optional[int] = None
    crawl: Optional[str] = None

    @property
    def server_limit(self) -> Optional[int]:
        """The limit sent to the server: only for a single request; with several, the captures selected by more
        than one request are removed first (and the limit is applied by IndexBackend.records())."""
        return self.limit if len(self.requests) == 1 else None

    def describe(self) -> str:
        lines = []
        for request in self.requests:
            params = ' '.join(f'{k}={v}' for k, v in request.params_with(self.limit).items())
            lines.append(f'{self.source}: url={request.url} {params}'.rstrip())
        if self.crawl:
            lines.append(f'crawls: {self.crawl}')
        return '\n'.join(lines) + '\n'


def api_params(query: IndexQuery, match_type: Optional[str]) -> dict:
    """The parameters of a CDX API request (CDXFetcher.iter/get) for one URL selector."""
    params = {}
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
