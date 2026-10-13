from abc import ABC, abstractmethod
from itertools import islice
import logging
from typing import FrozenSet, Iterable, Iterator, Mapping

from .query import IndexQuery, field_flag


LOGGER = logging.getLogger(__name__)


class UsageError(ValueError):
    """Invalid query or options (the command line reports it without a traceback)."""


class QueryPlan(ABC):
    """A query in the form of one backend, e.g. SQL text or SURT prefixes; what --dry-run shows."""

    @abstractmethod
    def describe(self) -> str:
        raise NotImplementedError  # pragma: no cover


class IndexBackend(ABC):
    """An index that can be queried for captures: the CDX API, CDX files, or the columnar index (Athena, DuckDB).

    A query (IndexQuery, the same for every backend) becomes the backend's plan (plan()), which is run by
    select() and select_count(), or shown by explain(). The public methods records(), count() and explain()
    validate the query first."""

    name: str = ''
    supported_options: FrozenSet[str] = frozenset()
    # True if all records have the same fields (in the same order), e.g. the columns of a SQL result
    fixed_schema: bool = False

    def validate(self, query: IndexQuery) -> None:
        unsupported = sorted(query.options_used() - self.supported_options)
        if unsupported:
            options = ', '.join(field_flag(option) for option in unsupported)
            raise UsageError(f'{options} cannot be used with the {self.name} index')
        if query.limit is not None and query.limit < 1:
            raise UsageError(f'--limit must be at least 1, not {query.limit}')

    @abstractmethod
    def plan(self, query: IndexQuery) -> QueryPlan:
        """The query in the form of this backend."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def select(self, query: IndexQuery) -> Iterator[Mapping]:
        """Yield the selected captures as mappings with CDX field names (url, timestamp, filename, ...).

        A backend can push --limit down to the index, but need not stop at it (see records())."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def select_count(self, query: IndexQuery) -> int:
        """Cheap (possibly estimated) number of captures matching the query, at most --limit (see count())."""
        raise NotImplementedError  # pragma: no cover

    def records(self, query: IndexQuery) -> Iterator[Mapping]:
        """The captures of the query: at most --limit, then filtered by --url-fgrep/--url-fgrepv (client-side)."""
        self.validate(query)
        return filter_by_url(islice(self.select(query), query.limit), query.url_fgrep, query.url_fgrepv)

    def count(self, query: IndexQuery) -> int:
        """The number of captures matching the query, at most --limit; without the client-side
        --url-fgrep/--url-fgrepv, which cannot be combined with it."""
        self.validate(query)
        if query.url_fgrep or query.url_fgrepv:
            raise UsageError('--count cannot be used with --url-fgrep or --url-fgrepv')
        return self.select_count(query)

    def explain(self, query: IndexQuery) -> str:
        """Describe what would be queried (--dry-run), without querying the index."""
        self.validate(query)
        return self.plan(query).describe()


def filter_by_url(records: Iterable[Mapping], url_fgrep=None, url_fgrepv=None) -> Iterator[Mapping]:
    if not (url_fgrep or url_fgrepv):
        yield from records  # records need no url, e.g. of a --sql query that selects only the WARC location
        return
    for record in records:
        url = record.get('url')
        if url is None:
            raise UsageError('--url-fgrep and --url-fgrepv need captures with a url (e.g. SELECT url in --sql)')
        if url_fgrep and url_fgrep not in url:
            LOGGER.debug('skipping due to --url-fgrep: %s', url)
            continue
        if url_fgrepv and url_fgrepv in url:
            LOGGER.debug('skipping due to --url-fgrepv: %s', url)
            continue
        yield record
