import logging
from typing import Iterable, Iterator, Mapping, Optional

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.base import RangeJobSource
from cdx_toolkit.repackage.sources.sql_base import join_warc_url


logger = logging.getLogger(__name__)

WARC_LOCATION_FIELDS = ('filename', 'offset', 'length')


class MissingWarcLocation(ValueError):
    """A record has no WARC location (filename, offset, length), e.g. an Internet Archive capture."""


class RecordsSource(RangeJobSource):
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
