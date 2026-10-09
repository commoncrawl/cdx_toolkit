import logging
from typing import Iterator, List, Optional

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.base import RangeJobSource
from cdx_toolkit.repackage.sources.sql_base import (
    validate_result_columns,
    join_warc_url,
    REQUIRED_RESULT_COLUMNS,
)


logger = logging.getLogger(__name__)

try:
    import duckdb
    _HAS_DUCKDB = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    duckdb = None
    _HAS_DUCKDB = False


def _build_from_clause(index_path: str, crawls: Optional[List[str]]) -> str:
    """Build a read_parquet(...) FROM expression over the CC columnar index.

    When crawls are given we glob only those crawl partitions (the pruning lever);
    otherwise we glob every crawl (expensive -> the cost guard fires)."""
    base = index_path.rstrip('/')
    if crawls:
        globs = [f"'{base}/crawl={c}/subset=warc/*.parquet'" for c in crawls]
    else:
        globs = [f"'{base}/crawl=*/subset=warc/*.parquet'"]
    return f"read_parquet([{', '.join(globs)}], hive_partitioning=true)"


class DuckDbSource(RangeJobSource):
    """RangeJobs (or rows) from a SQL query run with DuckDB, e.g. over the CC columnar index on S3.

    AWS region/credentials come from the environment (the AWS credential chain); the
    public bucket is readable with valid credentials."""

    def __init__(self, *, query: str, warc_download_prefix: Optional[str] = None, region_name: str = 'us-east-1'):
        self.query = query
        self.warc_download_prefix = warc_download_prefix
        self.region_name = region_name

    def iter_rows(self, validate: bool = False) -> Iterator[dict]:
        """Run the query and yield the result rows as dicts (column name -> value)."""
        if not _HAS_DUCKDB:
            raise RuntimeError(
                'DuckDB engine requires optional dependencies. Install cdx_toolkit[duckdb].'
            )

        query = self.query
        logger.info('Executing DuckDB query: %s', query)

        con = duckdb.connect()
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
            con.execute(f"SET s3_region='{self.region_name}';")
            # CC timestamps are UTC (timestamp literals in queries, timestamps in the results)
            con.execute("SET TimeZone='UTC';")
            try:
                # find AWS credentials like boto3 does (env, ~/.aws profiles, SSO, instance roles)
                con.execute('INSTALL aws; LOAD aws;')
                con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{self.region_name}');")
            except Exception as e:  # pragma: no cover - no credentials or aws extension unavailable
                logger.debug('duckdb AWS credential chain not used: %r', e)
            # Be resilient to transient S3 read timeouts and throttling (503 SlowDown) on large parquet
            # partitions: up to 8 retries, waiting 1s, 2s, 4s, ... (about 4 minutes in total)
            for stmt in (
                'SET http_timeout=120000;', 'SET http_retries=8;', 'SET http_retry_wait_ms=1000;',
                'SET http_retry_backoff=2;',
            ):
                try:
                    con.execute(stmt)
                except Exception as e:  # pragma: no cover - setting unsupported on this duckdb
                    logger.debug('duckdb setting skipped: %s (%r)', stmt, e)
            if 'hf://buckets/' in query:
                # DuckDB cannot read Hugging Face Storage Buckets itself; use their fsspec filesystem.
                from huggingface_hub import HfFileSystem

                con.register_filesystem(HfFileSystem())
            cur = con.execute(query)

            col_names = [d[0] for d in cur.description]
            if validate:
                validate_result_columns(col_names)

            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(col_names, row))
        finally:
            con.close()

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        for row in self.iter_rows(validate=True):
            warc_filename = row['warc_filename']
            extra = {name: value for name, value in row.items() if name not in REQUIRED_RESULT_COLUMNS}
            yield RangeJob(
                url=join_warc_url(self.warc_download_prefix, warc_filename),
                offset=int(row['warc_record_offset']),
                length=int(row['warc_record_length']),
                filename=warc_filename,
                extra=extra or None,
            )
