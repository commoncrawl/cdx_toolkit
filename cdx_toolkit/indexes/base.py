from abc import ABC, abstractmethod
import logging
from typing import FrozenSet, Iterable, Iterator, Mapping

from .query import IndexQuery, field_flag


LOGGER = logging.getLogger(__name__)


class UsageError(ValueError):
    """Invalid combination of command line options, reported without a traceback."""


class IndexBackend(ABC):
    """An index that can be queried for captures (CDX API, and later CDX files, Athena, DuckDB)."""

    name: str = ''
    supported_options: FrozenSet[str] = frozenset()

    def validate(self, query: IndexQuery) -> None:
        unsupported = sorted(query.options_used() - self.supported_options)
        if unsupported:
            options = ', '.join(field_flag(option) for option in unsupported)
            raise UsageError(f'{options} cannot be used with the {self.name} index')

    @abstractmethod
    def iter_records(self, query: IndexQuery) -> Iterator[Mapping]:
        """Yield the selected captures as mappings with CDX field names (url, timestamp, filename, ...).

        Backends apply --limit themselves, so it can be pushed down to the index."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def count(self, query: IndexQuery) -> int:
        """Cheap (possibly estimated) number of captures matching the query."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def explain(self, query: IndexQuery) -> str:
        """Describe what would be queried (--dry-run), without querying the index."""
        raise NotImplementedError  # pragma: no cover


def iter_index_records(backend: IndexBackend, query: IndexQuery) -> Iterator[Mapping]:
    """Query a backend and apply the client-side filters that work for every backend."""
    return filter_by_url(backend.iter_records(query), query.url_fgrep, query.url_fgrepv)


def filter_by_url(records: Iterable[Mapping], url_fgrep=None, url_fgrepv=None) -> Iterator[Mapping]:
    for record in records:
        url = record['url']
        if url_fgrep and url_fgrep not in url:
            LOGGER.debug('skipping due to --url-fgrep: %s', url)
            continue
        if url_fgrepv and url_fgrepv in url:
            LOGGER.debug('skipping due to --url-fgrepv: %s', url)
            continue
        yield record
