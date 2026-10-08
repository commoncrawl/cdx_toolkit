import logging
from typing import Iterator, List, Optional

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.base import RangeJobSource, CostEstimate
from cdx_toolkit.repackage.sources.sql_base import (
    build_sql,
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
    """RangeJobs from a query against the CC columnar index via DuckDB (read_parquet on S3).

    Reads the public CommonCrawl parquet directly; AWS region/credentials come from
    the environment (the public bucket is readable with valid credentials)."""

    def __init__(
        self,
        *,
        query: Optional[str] = None,
        hostnames: Optional[List[str]] = None,
        domains: Optional[List[str]] = None,
        crawls: Optional[List[str]] = None,
        index_path: str,
        warc_download_prefix: Optional[str],
        limit: int = 0,
        sort: bool = True,
        region_name: str = 'us-east-1',
    ):
        self.raw_query = query
        self.hostnames = hostnames
        self.domains = domains
        self.crawls = crawls
        self.index_path = index_path
        self.warc_download_prefix = warc_download_prefix
        self.limit = limit
        self.sort = sort
        self.region_name = region_name

    def estimate_cost(self) -> CostEstimate:
        if self.raw_query is not None:
            return CostEstimate(n_crawls=None, engine='duckdb')
        return CostEstimate(n_crawls=len(self.crawls) if self.crawls else None, engine='duckdb')

    def _build_query(self) -> str:
        if self.raw_query is not None:
            return self.raw_query
        from_clause = _build_from_clause(self.index_path, self.crawls)
        # crawl pruning is done in the FROM glob, so no crawl IN (...) in the WHERE
        return build_sql(
            from_clause, self.hostnames, crawls=None, limit=self.limit,
            url_host_registered_domains=self.domains, order_by=self.sort,
        )

    def iter_rows(self, validate: bool = False) -> Iterator[dict]:
        """Run the query and yield the result rows as dicts (column name -> value)."""
        if not _HAS_DUCKDB:
            raise RuntimeError(
                'DuckDB engine requires optional dependencies. Install cdx_toolkit[duckdb].'
            )

        query = self._build_query()
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
            # Be resilient to transient S3 read timeouts on large parquet partitions.
            for stmt in ('SET http_timeout=120000;', 'SET http_retries=5;'):
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
