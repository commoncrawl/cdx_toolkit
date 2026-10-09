"""AthenaSource and the Athena index backend with a fake boto3 Athena client (no AWS)."""
import json
import logging
from unittest.mock import patch

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.indexes import IndexQuery
from cdx_toolkit.indexes.sql import AthenaBackend
from cdx_toolkit.repackage.sources.athena import AthenaSource, report_query_cost, run_athena_query

ATHENA = 'athena://athena.eu-west-1.amazonaws.com/mydb?S3OutputLocation=s3://bucket/athena/'
COLUMNS = ['url', 'fetch_time', 'fetch_status', 'warc_filename', 'warc_record_offset', 'warc_record_length']
ROWS = [
    ['https://example.com/', '2024-07-13 10:22:33.000', '200', 'crawl-data/a.warc.gz', '10', '100'],
    ['https://example.com/x', '2024-07-14 08:00:00.000', '404', 'crawl-data/b.warc.gz', '20', '200'],
]


class FakeAthenaClient:
    """start_query_execution / get_query_execution (states over time) / paginated results."""

    def __init__(self, states=('RUNNING', 'SUCCEEDED'), rows=ROWS, scanned=2 * 10 ** 9, reason=None):
        self.states = list(states)
        self.rows = rows
        self.scanned = scanned
        self.reason = reason
        self.started = []
        self.stopped = []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {'QueryExecutionId': 'qid'}

    def get_query_execution(self, QueryExecutionId):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        execution = {'Status': {'State': state}}
        if self.reason:
            execution['Status']['StateChangeReason'] = self.reason
        if self.scanned is not None:
            execution['Statistics'] = {'DataScannedInBytes': self.scanned}
        return {'QueryExecution': execution}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)

    def get_paginator(self, name):
        assert name == 'get_query_results'
        header = {'Data': [{'VarCharValue': c} for c in COLUMNS]}
        data = [{'Data': [{'VarCharValue': v} for v in row]} for row in self.rows]
        # two pages: the header is only on the first one
        pages = [{'ResultSet': {'Rows': [header] + data[:1]}}, {'ResultSet': {'Rows': data[1:]}}]
        return type('Paginator', (), {'paginate': lambda self, **kwargs: iter(pages)})()


@pytest.fixture
def client():
    fake = FakeAthenaClient()
    with patch.object(AthenaSource, '_make_client', return_value=fake), \
            patch('cdx_toolkit.repackage.sources.athena.time.sleep'):
        yield fake


def test_source_rows_jobs_and_cost(client, caplog):
    source = AthenaSource(query='SELECT 1', database='ccindex', s3_output_location='s3://b/p/',
                          warc_download_prefix='s3://commoncrawl')

    with caplog.at_level(logging.INFO):
        rows = list(source.iter_rows())
    assert rows == [dict(zip(COLUMNS, row)) for row in ROWS]
    assert client.started[0]['QueryExecutionContext'] == {'Database': 'ccindex'}
    assert client.started[0]['ResultConfiguration'] == {'OutputLocation': 's3://b/p/'}
    assert 'Athena scanned 2.00 GB, estimated cost ~$0.0100' in caplog.text

    jobs = list(source.iter_range_jobs())
    assert [(j.url, j.offset, j.length) for j in jobs] == [
        ('s3://commoncrawl/crawl-data/a.warc.gz', 10, 100), ('s3://commoncrawl/crawl-data/b.warc.gz', 20, 200),
    ]
    assert jobs[0].extra == {
        'url': 'https://example.com/', 'fetch_time': '2024-07-13 10:22:33.000', 'fetch_status': '200',
    }


def test_index_athena_end_to_end(client, capsys):
    main(['index', ATHENA, '--hostname', 'example.com', '--yes'])

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert records[0] == {
        'url': 'https://example.com/', 'timestamp': '20240713102233', 'status': '200',
        'filename': 'crawl-data/a.warc.gz', 'offset': '10', 'length': '100',
    }
    assert len(records) == 2
    assert 'FROM ccindex' in client.started[0]['QueryString']
    assert client.started[0]['QueryExecutionContext'] == {'Database': 'mydb'}


def test_athena_count():
    fake = FakeAthenaClient(rows=[['2']])
    fake.get_paginator = lambda name: type('P', (), {'paginate': lambda self, **kw: iter([
        {'ResultSet': {'Rows': [{'Data': [{'VarCharValue': 'n'}]}, {'Data': [{'VarCharValue': '42'}]}]}}
    ])})()
    with patch.object(AthenaSource, '_make_client', return_value=fake), \
            patch('cdx_toolkit.repackage.sources.athena.time.sleep'):
        assert AthenaBackend(ATHENA, assume_yes=True).count(IndexQuery(domains=['example.com'])) == 42
    assert fake.started[0]['QueryString'].startswith('SELECT count(*) AS n FROM (')


def test_failed_query(caplog):
    fake = FakeAthenaClient(states=['FAILED'], reason='SYNTAX_ERROR: line 1:8')
    with caplog.at_level(logging.INFO), pytest.raises(Exception, match='Query failed with status: FAILED'):
        run_athena_query(fake, 'SELECT', 'ccindex', 's3://b/p/')
    assert 'SYNTAX_ERROR' in caplog.text
    assert fake.stopped == []


def test_query_timeout_is_cancelled():
    fake = FakeAthenaClient(states=['RUNNING'])
    with patch('cdx_toolkit.repackage.sources.athena.time.sleep'), pytest.raises(TimeoutError):
        run_athena_query(fake, 'SELECT 1', 'ccindex', 's3://b/p/', max_wait_time=0)
    assert fake.stopped == ['qid']


def test_cost_report_without_statistics(caplog):
    with caplog.at_level(logging.INFO):
        report_query_cost(FakeAthenaClient(scanned=None), 'qid')
    assert 'Athena scanned' not in caplog.text


def test_make_client_uses_the_region():
    pytest.importorskip('boto3')
    source = AthenaSource(query='SELECT 1', database='d', s3_output_location='s3://b/', warc_download_prefix=None,
                          region_name='eu-west-1')
    with patch('boto3.client') as boto3_client:
        source._make_client()
    args, kwargs = boto3_client.call_args
    assert args == ('athena',) and kwargs['config'].region_name == 'eu-west-1'
