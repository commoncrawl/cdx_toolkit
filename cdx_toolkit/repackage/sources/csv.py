import csv
import logging
from typing import Iterator, Optional

import fsspec

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.base import RangeJobSource
from cdx_toolkit.repackage.sources.sql_base import join_warc_url


logger = logging.getLogger(__name__)

# Fetch-job columns name the WARC *location* (mirroring the columnar index's column
# names) so they never collide with extra analysis columns a raw --query may SELECT
# -- e.g. the index's own `url` (the captured page URL). `warc_filename` is a filename
# relative to the WARC download prefix, or a full URL (s3://, https://, hf://...).
FILENAME_FIELDS = ['warc_filename', 'warc_record_offset', 'warc_record_length']

# Columns the reader interprets as the fetch job; everything else round-trips as extra.
_KNOWN_FIELDS = set(FILENAME_FIELDS)


class RangeJobCsvWriter:
    """Write RangeJobs to a CSV (local or remote via fsspec).

    The `warc_filename` column holds the full download URL of each job, so the CSV can
    be read without knowing the WARC download prefix. Any extra columns carried on a
    RangeJob (from a raw SQL --query) are appended after the fetch-job columns. The
    header is written lazily from the first job so its extra columns can be discovered."""

    def __init__(self, path: str):
        self.path = path
        self._ctx = fsspec.open(path, 'wt', newline='')
        self._fh = self._ctx.__enter__()
        self._writer = None

    def _init_writer(self, fieldnames) -> None:
        self._writer = csv.DictWriter(
            self._fh, fieldnames=fieldnames, extrasaction='ignore', restval=''
        )
        self._writer.writeheader()

    def write(self, job: RangeJob) -> None:
        row = {
            'warc_filename': job.url,
            'warc_record_offset': job.offset,
            'warc_record_length': job.length,
        }
        if job.extra:
            row.update(job.extra)

        if self._writer is None:
            extra_fields = [k for k in (job.extra or {}) if k not in FILENAME_FIELDS]
            self._init_writer(FILENAME_FIELDS + extra_fields)
        self._writer.writerow(row)

    def close(self) -> None:
        if self._ctx is not None:
            # No rows written: still emit the base header for a valid (empty) file.
            if self._writer is None:
                self._init_writer(FILENAME_FIELDS)
            self._ctx.__exit__(None, None, None)
            self._ctx = None
            self._fh = None


class CsvSource(RangeJobSource):
    """RangeJobs read from a CSV/TSV with `warc_filename`, `warc_record_offset` and
    `warc_record_length` columns.

    A `warc_filename` that is a full URL (s3://, https://, hf://...) is used as-is,
    otherwise the WARC download prefix is prepended. Any other columns round-trip onto
    RangeJob.extra. TSV is detected from a `.tsv`/`.tsv.gz` extension; `.gz` inputs are
    decompressed.

    With `sort` (the default), jobs are sorted by (WARC file, record offset) before
    being emitted -- grouping same-file records with ascending offsets for better S3
    range-read locality. This buffers all rows in memory; pass sort=False to stream
    an already-sorted file without buffering."""

    def __init__(self, path: str, warc_download_prefix: Optional[str], sort: bool = True):
        self.path = path
        self.warc_download_prefix = warc_download_prefix
        self.sort = sort

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        path = str(self.path)
        delimiter = '\t' if path.endswith(('.tsv', '.tsv.gz')) else ','
        compression = 'gzip' if path.endswith('.gz') else None

        buffered = [] if self.sort else None
        with fsspec.open(self.path, 'rt', newline='', compression=compression) as fh:
            reader = csv.DictReader(fh, delimiter=delimiter)
            missing = [f for f in FILENAME_FIELDS if f not in (reader.fieldnames or [])]
            if missing:
                raise ValueError(
                    f'range-jobs CSV {self.path} must have the columns {", ".join(FILENAME_FIELDS)} '
                    f'(got header: {reader.fieldnames})'
                )

            for row in reader:
                filename = row['warc_filename']
                extra = {k: v for k, v in row.items() if k not in _KNOWN_FIELDS}
                job = RangeJob(
                    url=join_warc_url(self.warc_download_prefix, filename),
                    offset=int(row['warc_record_offset']),
                    length=int(row['warc_record_length']),
                    filename=filename,
                    extra=extra or None,
                )
                if buffered is None:
                    yield job
                else:
                    buffered.append(job)

        if buffered is not None:
            buffered.sort(key=lambda j: (j.filename or j.url or '', j.offset))
            for job in buffered:
                yield job
