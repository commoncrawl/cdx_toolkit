'''
Running queries with AWS Athena (boto3).
'''
import logging
import time
from typing import Iterable, Iterator, Optional

from cdx_toolkit.aws import botocore_config


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
