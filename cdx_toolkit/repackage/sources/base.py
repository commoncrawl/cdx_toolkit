from abc import ABC, abstractmethod
from typing import Iterator, NamedTuple, Optional

from cdx_toolkit.repackage.data_classes import RangeJob


class CostEstimate(NamedTuple):
    """Describes the scan a source is about to run, for the cost-confirmation guard.

    n_crawls is the number of crawls the scan is bounded to, or None when the scan
    is unbounded / its pruning cannot be verified (e.g. raw SQL, all-crawls glob)."""

    n_crawls: Optional[int]
    engine: str


class RangeJobSource(ABC):
    """A source of RangeJobs for the repackage pipeline.

    A source owns its own stage-1 resource (Athena client, DuckDB connection, or an
    fsspec file handle) and yields RangeJobs synchronously; the pipeline orchestrator
    bridges the sync generator into the async fetch/write stages, and owns queueing,
    counting, and stop-sentinel emission."""

    @abstractmethod
    def iter_range_jobs(self) -> Iterator[RangeJob]:
        """Yield RangeJobs. Implementations open their own client/connection lazily
        and close it in a finally. Each RangeJob carries `url` (authoritative for
        fetching) and, where known, the relative `filename`."""
        raise NotImplementedError  # pragma: no cover
