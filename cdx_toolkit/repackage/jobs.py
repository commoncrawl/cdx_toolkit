'''
The captures to repackage: index records (as written by `cdxt index`) -> jobs (a WARC range, or a wayback
replay), and the result of a run.
'''
import logging
from dataclasses import dataclass, field
from typing import Iterable, Iterator, Mapping, Optional, Union


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepackageResult:
    """Number of captures to repackage, written to WARC files, and failed (not fetched)."""

    requested: int = 0
    written: int = 0
    failed: int = 0

    def __add__(self, other: 'RepackageResult') -> 'RepackageResult':
        return RepackageResult(
            self.requested + other.requested, self.written + other.written, self.failed + other.failed
        )


@dataclass(frozen=True)
class RangeJob:
    """The bytes offset .. offset+length-1 of a WARC file (one or more WARC records)."""

    url: str
    offset: int
    length: int
    records_count: int = 1
    # WARC filename of the record as given in the index (e.g. crawl-data/...warc.gz, relative to the download
    # prefix), used to sort and shard the jobs by WARC file; `url` is authoritative for fetching.
    filename: Optional[str] = None

    def __str__(self) -> str:
        return f'{self.url} [{self.offset},{self.length}]'


@dataclass(frozen=True)
class WaybackJob:
    """A capture replayed from a wayback machine (`url` is its replay URL), written as one WARC record."""

    url: str
    capture: Mapping = field(compare=False)
    wb: str
    records_count: int = 1
    filename: Optional[str] = None  # no WARC file

    def __str__(self) -> str:
        return self.url


Job = Union[RangeJob, WaybackJob]


@dataclass(frozen=True)
class RangePayload:
    """The bytes read for a job (one or more gzipped WARC records)."""

    job: Job
    data: bytes


WARC_LOCATION_FIELDS = ('filename', 'offset', 'length')


class MissingWarcLocation(ValueError):
    """A record has no WARC location (filename, offset, length), e.g. an Internet Archive capture."""


def join_warc_url(prefix: Optional[str], warc_filename: str) -> str:
    """Join a download prefix with a warc_filename robustly.

    - if warc_filename is already absolute (contains '://'), return it unchanged
      (supports custom queries / self-contained index files whose value is a full URL);
    - if prefix is empty/None, return warc_filename unchanged;
    - otherwise join with exactly one '/' (no double slash, no missing slash)."""
    if '://' in warc_filename:
        return warc_filename
    if not prefix:
        return warc_filename
    return prefix.rstrip('/') + '/' + warc_filename.lstrip('/')


class RecordsSource:
    """RangeJobs from index records (as written by `cdxt index`) with filename, offset and length fields.

    With `sort` (the default), jobs are sorted by (WARC file, record offset) for better
    range-read locality. The records are then read (and validated) when the source is
    created, and kept in memory; pass sort=False to stream them in input order."""

    def __init__(self, records: Iterable[Mapping], warc_download_prefix: Optional[str], sort: bool = True):
        self.warc_download_prefix = warc_download_prefix
        self.jobs = (self.to_range_job(record) for record in records)
        if sort:
            self.jobs = sorted(self.jobs, key=lambda j: (j.filename or j.url, j.offset))

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        """Yield the range jobs; `url` is authoritative for fetching, `filename` is the filename of the record."""
        yield from self.jobs

    def to_range_job(self, record: Mapping) -> RangeJob:
        if not all(record.get(field) not in (None, '') for field in WARC_LOCATION_FIELDS):
            raise MissingWarcLocation(
                f'capture {record.get("url")} has no WARC location (filename, offset, length); '
                'use --wb to fetch it from a wayback machine'
            )
        return RangeJob(
            url=join_warc_url(self.warc_download_prefix, record['filename']),
            offset=int(record['offset']),
            length=int(record['length']),
            filename=record['filename'],
        )


# wayback names that map to replay URLs
WAYBACK_NAMES = {'ia': 'https://web.archive.org/web'}


class WaybackSource:
    """WaybackJobs from index records (url, timestamp, status), in the order of the records: the captures are
    replayed from the wayback machine `wb` (a replay URL prefix, or a name of WAYBACK_NAMES)."""

    def __init__(self, records: Iterable[Mapping], wb: str):
        from cdx_toolkit.warc import wb_replay_url

        self.wb = WAYBACK_NAMES.get(wb, wb)
        self.jobs = (WaybackJob(wb_replay_url(record, self.wb), record, self.wb) for record in records)

    def iter_range_jobs(self) -> Iterator[WaybackJob]:
        yield from self.jobs
