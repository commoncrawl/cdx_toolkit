'''
Index backend for the columnar URL index queried with AWS Athena (boto3).
'''
from dataclasses import dataclass
import logging
import re
import time
from typing import Iterator, Optional
from urllib.parse import parse_qs, urlparse

from cdx_toolkit.aws import botocore_config

from ..base import UsageError
from .sql import SqlBackend


logger = logging.getLogger(__name__)

# Athena pricing is ~$5 per TB scanned (used for the post-run cost estimate).
ATHENA_USD_PER_TB = 5.0


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
        self.athena = AthenaLocation.parse(location)

    def from_clause(self, crawls):
        return self.athena.table, False

    def iter_sql_rows(self, sql):
        client = AthenaClient(self.athena.database, self.athena.s3_output_location, region_name=self.athena.region)
        return client.iter_rows(sql)


@dataclass(frozen=True)
class AthenaLocation:
    """Where an index is queried with Athena, and where Athena writes the results."""

    region: str
    database: str
    table: str
    s3_output_location: str

    @classmethod
    def parse(cls, location: str) -> 'AthenaLocation':
        """athena://athena.us-east-1.amazonaws.com:443/ccindex?S3OutputLocation=s3://bucket/path/[&table=ccindex]"""
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
        return cls(match.group(1), database, table, s3_output_location)


def make_client(region_name: Optional[str] = None):
    import boto3

    # retries: Common Crawl's recommended AWS settings, unless configured by the user (see cdx_toolkit.aws)
    return boto3.client('athena', config=botocore_config(region_name=region_name, read_timeout=60))


class AthenaClient:
    """Runs queries in an Athena database; Athena writes the results to `s3_output_location`."""

    def __init__(self, database: str, s3_output_location: str, region_name: Optional[str] = None, client=None,
                 max_wait_time: int = 300):
        self.database = database
        self.s3_output_location = s3_output_location
        self.client = client or make_client(region_name)
        self.max_wait_time = max_wait_time

    def iter_rows(self, sql: str) -> Iterator[dict]:
        """Run the query and yield the result rows as dicts (column name -> string value)."""
        query_execution_id = self.run(sql)
        try:
            yield from self.rows(query_execution_id)
        finally:
            self.report_cost(query_execution_id)

    def run(self, sql: str) -> str:
        """Start an Athena query and block until it completes; return the execution id.

        Raises if the query does not reach the SUCCEEDED state. If the wait is
        interrupted (Ctrl-C / cancellation) or times out, the query is cancelled
        server-side so we don't keep paying for a scan whose results we'll never read."""
        logger.info('Executing Athena query: %s', sql)

        response = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_output_location},
        )

        query_execution_id = response['QueryExecutionId']
        logger.info('Query execution started. ID: %s', query_execution_id)

        try:
            status = self._wait_for_completion(query_execution_id)
        except BaseException:
            # Ctrl-C, asyncio cancellation, or timeout: stop the query server-side
            # to bound the Athena scan cost, then propagate the original exception.
            self._stop(query_execution_id)
            raise

        if status != 'SUCCEEDED':
            raise Exception(f'Query failed with status: {status}')

        return query_execution_id

    def rows(self, query_execution_id: str) -> Iterator[dict]:
        """The results of a completed query as dicts (column name -> string value)."""
        paginator = self.client.get_paginator('get_query_results')
        column_names = None

        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            rows = page['ResultSet']['Rows']

            # Get column names from first page
            if column_names is None and rows:
                column_names = [col['VarCharValue'] for col in rows[0]['Data']]
                rows = rows[1:]  # Skip header row

            for row in rows:
                row_data = [cell.get('VarCharValue', None) for cell in row['Data']]
                yield dict(zip(column_names, row_data))

    def report_cost(self, query_execution_id: str) -> None:
        """Log the bytes scanned and an estimated USD cost for a completed query."""
        try:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            scanned = response['QueryExecution'].get('Statistics', {}).get('DataScannedInBytes')
        except Exception as e:  # pragma: no cover - best-effort reporting
            logger.debug('unable to read Athena query statistics: %r', e)
            return

        if scanned is None:
            return

        gb = scanned / 1e9
        usd = scanned / 1e12 * ATHENA_USD_PER_TB
        logger.info('Athena scanned %.2f GB, estimated cost ~$%.4f', gb, usd)

    def _wait_for_completion(self, query_execution_id: str) -> str:
        """Wait for query to complete and return final status"""
        start_time = time.time()

        while time.time() - start_time < self.max_wait_time:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)

            status = response['QueryExecution']['Status']['State']
            logger.info(f'Query status: {status}')

            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'FAILED':
                    error_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.info(f'Query failed: {error_reason}')
                return status

            time.sleep(2)

        raise TimeoutError(f'Query did not complete within {self.max_wait_time} seconds')

    def _stop(self, query_execution_id: str) -> None:
        """Best-effort cancellation of a running Athena query."""
        logger.warning('Cancelling Athena query %s ...', query_execution_id)
        try:
            self.client.stop_query_execution(QueryExecutionId=query_execution_id)
            logger.warning('Athena query %s cancelled', query_execution_id)
        except Exception as e:  # pragma: no cover - best-effort cleanup
            logger.warning('Failed to cancel Athena query %s: %r', query_execution_id, e)
