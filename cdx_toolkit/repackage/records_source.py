'''
The captures to repackage: index records (as written by `cdxt index`) -> range jobs for the WARC fetcher.
'''
import logging
from typing import Iterable, Iterator, Mapping, Optional

from cdx_toolkit.repackage.data_classes import RangeJob


logger = logging.getLogger(__name__)

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
