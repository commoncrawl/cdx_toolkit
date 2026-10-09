import itertools
import logging
import time
from typing import Iterator, Iterable, Optional

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.base import RangeJobSource
from cdx_toolkit.repackage.sources.sql_base import (
    validate_result_columns,
    join_warc_url,
    REQUIRED_RESULT_COLUMNS,
)


logger = logging.getLogger(__name__)

# Athena pricing is ~$5 per TB scanned (used for the post-run cost estimate).
ATHENA_USD_PER_TB = 5.0


class AthenaSource(RangeJobSource):
    """RangeJobs from a query against the CC columnar index via AWS Athena."""

    def __init__(
        self,
        *,
        query: str,
        database: str,
        s3_output_location: str,
        warc_download_prefix: Optional[str],
        region_name: str = 'us-east-1',
        max_wait_time: int = 300,
    ):
        self.query = query
        self.database = database
        self.s3_output_location = s3_output_location
        self.warc_download_prefix = warc_download_prefix
        self.region_name = region_name
        self.max_wait_time = max_wait_time

    def _make_client(self):
        import boto3
        from botocore.config import Config

        config = Config(
            region_name=self.region_name,
            read_timeout=60,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
        )
        return boto3.client('athena', config=config)

    def iter_rows(self) -> Iterator[dict]:
        """Run the query and yield the result rows as dicts (column name -> string value)."""
        client = self._make_client()
        query_execution_id = run_athena_query(
            client, self.query, self.database, self.s3_output_location, self.max_wait_time
        )
        try:
            yield from iter_result_rows(client, query_execution_id)
        finally:
            report_query_cost(client, query_execution_id)

    def iter_range_jobs(self) -> Iterator[RangeJob]:
        rows = self.iter_rows()
        first = next(rows, None)
        if first is not None:
            validate_result_columns(list(first))
            for row in itertools.chain([first], rows):
                yield row_to_range_job(row, self.warc_download_prefix)


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


def row_to_range_job(row: dict, warc_download_prefix: Optional[str]) -> RangeJob:
    warc_filename = row['warc_filename']
    extra = {k: v for k, v in row.items() if k not in REQUIRED_RESULT_COLUMNS}
    return RangeJob(
        url=join_warc_url(warc_download_prefix, warc_filename),
        offset=int(row['warc_record_offset']),
        length=int(row['warc_record_length']),
        filename=warc_filename,
        extra=extra or None,
    )


def iter_range_jobs(client, query_execution_id: str, warc_download_prefix: Optional[str]) -> Iterable[RangeJob]:
    """Retrieve query results and convert each row to a RangeJob"""
    rows = iter_result_rows(client, query_execution_id)
    first = next(rows, None)
    if first is None:
        return
    validate_result_columns(list(first))
    for row in itertools.chain([first], rows):
        yield row_to_range_job(row, warc_download_prefix)
