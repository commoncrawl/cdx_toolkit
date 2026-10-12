'''
Running queries with DuckDB, e.g. over the columnar URL index on S3 or on Hugging Face.
'''
import logging
from typing import Iterator, List, Optional

from cdx_toolkit.aws import aws_region, duckdb_settings


logger = logging.getLogger(__name__)

try:
    import duckdb
    _HAS_DUCKDB = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    duckdb = None
    _HAS_DUCKDB = False


def build_from_clause(index_path: str, crawls: Optional[List[str]]) -> str:
    """Build a read_parquet(...) FROM expression over the CC columnar index.

    When crawls are given we glob only those crawl partitions (the pruning lever);
    otherwise we glob every crawl (expensive -> the cost guard fires)."""
    base = index_path.rstrip('/')
    if crawls:
        globs = [f"'{base}/crawl={c}/subset=warc/*.parquet'" for c in crawls]
    else:
        globs = [f"'{base}/crawl=*/subset=warc/*.parquet'"]
    return f"read_parquet([{', '.join(globs)}], hive_partitioning=true)"


def _configure_httpfs(con, s3: bool, region_name: Optional[str] = None) -> None:
    """Load the httpfs extension (http(s):// and s3://); for s3://, also find AWS credentials and apply the S3
    settings."""
    con.execute('INSTALL httpfs; LOAD httpfs;')
    settings = ['SET http_timeout=120;']  # 2 minutes for large parquet partitions
    if s3:
        region = aws_region(region_name)
        try:
            # find AWS credentials like boto3 does (env, ~/.aws profiles, SSO, instance roles)
            con.execute('INSTALL aws; LOAD aws;')
            con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{region}');")
        except Exception as e:  # pragma: no cover - no credentials or aws extension unavailable
            logger.debug('duckdb AWS credential chain not used: %r', e)
        # region, retries and URL style: Common Crawl's recommended S3 settings, unless configured by
        # the user (see cdx_toolkit.aws)
        settings = duckdb_settings(region_name) + settings
    for stmt in settings:
        try:
            con.execute(stmt)
        except Exception as e:  # pragma: no cover - setting unsupported on this duckdb
            logger.debug('duckdb setting skipped: %s (%r)', stmt, e)


def iter_duckdb_rows(query: str, region_name: Optional[str] = None) -> Iterator[dict]:
    """Run the query and yield the result rows as dicts (column name -> value).

    AWS region/credentials come from the environment (the AWS credential chain); the
    public bucket is readable with valid credentials."""
    if not _HAS_DUCKDB:
        raise RuntimeError(
            'DuckDB engine requires optional dependencies. Install cdx_toolkit[duckdb].'
        )

    logger.info('Executing DuckDB query: %s', query)

    con = duckdb.connect()
    try:
        # CC timestamps are UTC (timestamp literals in queries, timestamps in the results)
        con.execute("SET TimeZone='UTC';")
        # remote files only as needed: a local query needs no extensions (nor AWS credentials)
        if 's3://' in query or 'http://' in query or 'https://' in query:
            _configure_httpfs(con, s3='s3://' in query, region_name=region_name)
        if 'hf://buckets/' in query:
            # DuckDB cannot read Hugging Face Storage Buckets itself; use their fsspec filesystem.
            from huggingface_hub import HfFileSystem

            con.register_filesystem(HfFileSystem())
        cur = con.execute(query)

        col_names = [d[0] for d in cur.description]

        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield dict(zip(col_names, row))
    finally:
        con.close()
