'''
Index backend for the columnar URL index queried with AWS Athena (boto3).
'''
import logging
import re
import time
from typing import Iterable, Iterator, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from cdx_toolkit.aws import botocore_config

from ..base import UsageError
from .sql import SqlBackend


logger = logging.getLogger(__name__)

# Athena pricing is ~$5 per TB scanned (used for the post-run cost estimate).
ATHENA_USD_PER_TB = 5.0


def make_client(region_name: Optional[str] = None):
    import boto3

    # retries: Common Crawl's recommended AWS settings, unless configured by the user (see cdx_toolkit.aws)
    return boto3.client('athena', config=botocore_config(region_name=region_name, read_timeout=60))


def iter_athena_rows(query: str, database: str, s3_output_location: str, region_name: Optional[str] = None,
                     max_wait_time: int = 300) -> Iterator[dict]:
    """Run the query and yield the result rows as dicts (column name -> string value)."""
    client = make_client(region_name)
    query_execution_id = run_athena_query(client, query, database, s3_output_location, max_wait_time)
    try:
        yield from iter_result_rows(client, query_execution_id)
    finally:
        report_query_cost(client, query_execution_id)


def run_athena_query(client, query: str, database: str, s3_output_location: str, max_wait_time: int = 300) -> str:
    """Start an Athena query and block until it completes; return the execution id.

    Raises if the query does not reach the SUCCEEDED state. If the wait is
    interrupted (Ctrl-C / cancellation) or times out, the query is cancelled
    server-side so we don't keep paying for a scan whose results we'll never read."""
    logger.info('Executing Athena query: %s', query)

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output_location},
    )

    query_execution_id = response['QueryExecutionId']
    logger.info('Query execution started. ID: %s', query_execution_id)

    try:
        status = _wait_for_query_completion(client, query_execution_id, max_wait_time)
    except BaseException:
        # Ctrl-C, asyncio cancellation, or timeout: stop the query server-side
        # to bound the Athena scan cost, then propagate the original exception.
        _stop_query(client, query_execution_id)
        raise

    if status != 'SUCCEEDED':
        raise Exception(f'Query failed with status: {status}')

    return query_execution_id


def _stop_query(client, query_execution_id: str) -> None:
    """Best-effort cancellation of a running Athena query."""
    logger.warning('Cancelling Athena query %s ...', query_execution_id)
    try:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        logger.warning('Athena query %s cancelled', query_execution_id)
    except Exception as e:  # pragma: no cover - best-effort cleanup
        logger.warning('Failed to cancel Athena query %s: %r', query_execution_id, e)


def report_query_cost(client, query_execution_id: str) -> None:
    """Log the bytes scanned and an estimated USD cost for a completed query."""
    try:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        scanned = response['QueryExecution'].get('Statistics', {}).get('DataScannedInBytes')
    except Exception as e:  # pragma: no cover - best-effort reporting
        logger.debug('unable to read Athena query statistics: %r', e)
        return

    if scanned is None:
        return

    gb = scanned / 1e9
    usd = scanned / 1e12 * ATHENA_USD_PER_TB
    logger.info('Athena scanned %.2f GB, estimated cost ~$%.4f', gb, usd)


def _wait_for_query_completion(client, query_execution_id: str, max_wait_time: int) -> str:
    """Wait for query to complete and return final status"""
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)

        status = response['QueryExecution']['Status']['State']
        logger.info(f'Query status: {status}')

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'FAILED':
                error_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.info(f'Query failed: {error_reason}')
            return status

        time.sleep(2)

    raise TimeoutError(f'Query did not complete within {max_wait_time} seconds')


def iter_result_rows(client, query_execution_id: str) -> Iterable[dict]:
    """Retrieve query results as dicts (column name -> string value)"""
    paginator = client.get_paginator('get_query_results')
    page_iterator = paginator.paginate(QueryExecutionId=query_execution_id)
    column_names = None

    for page in page_iterator:
        rows = page['ResultSet']['Rows']

        # Get column names from first page
        if column_names is None and rows:
            column_names = [col['VarCharValue'] for col in rows[0]['Data']]
            rows = rows[1:]  # Skip header row

        # Process data rows
        for row in rows:
            row_data = [cell.get('VarCharValue', None) for cell in row['Data']]
            yield dict(zip(column_names, row_data))


class AthenaBackend(SqlBackend):
    """The columnar index queried with AWS Athena.

    Location: athena://athena.<region>.amazonaws.com[:443]/<database>?S3OutputLocation=s3://...[&table=ccindex]
    """

    name = 'Athena'
    engine = 'athena'
    url_file_hint = (
        ' (after creating an Athena table `target_urls` from the URL file on S3):\n\n'
        '  SELECT idx.url, idx.warc_filename, idx.warc_record_offset, idx.warc_record_length\n'
        '  FROM ccindex AS idx\n'
        '  JOIN target_urls AS target ON idx.url = target.url\n'
        "  WHERE idx.crawl = 'CC-MAIN-2024-33' AND idx.subset = 'warc'"
    )

    def __init__(self, location: str, assume_yes: bool = False):
        super().__init__(location, assume_yes)
        self.region, self.database, self.table, self.s3_output_location = parse_athena_location(location)

    def from_clause(self, crawls):
        return self.table, False

    def iter_sql_rows(self, sql):
        return iter_athena_rows(sql, self.database, self.s3_output_location, region_name=self.region)


def parse_athena_location(location: str) -> Tuple[str, str, str, str]:
    """athena://athena.us-east-1.amazonaws.com:443/ccindex?S3OutputLocation=s3://bucket/path/
    -> (region, database, table, S3 output location)"""
    parsed = urlparse(location)
    match = re.fullmatch(r'athena\.([a-z0-9-]+)\.amazonaws\.com', parsed.hostname or '')
    if parsed.scheme != 'athena' or not match:
        raise UsageError(
            f'invalid Athena location {location!r}, expected '
            'athena://athena.<region>.amazonaws.com/<database>?S3OutputLocation=s3://<bucket>/<path>/'
        )
    params = parse_qs(parsed.query)
    s3_output_location = params.get('S3OutputLocation', [None])[0]
    if not s3_output_location or not s3_output_location.startswith('s3://'):
        raise UsageError(f'the Athena location {location!r} needs a S3OutputLocation=s3://... parameter')
    database = parsed.path.strip('/') or 'ccindex'
    table = params.get('table', ['ccindex'])[0]
    return match.group(1), database, table, s3_output_location
