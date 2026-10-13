"""Running Athena queries and the Athena index backend with a fake boto3 Athena client (no AWS)."""
import json
import logging
from unittest.mock import patch

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.indexes import IndexQuery
from cdx_toolkit.indexes.backends.athena import (
    AthenaBackend, iter_athena_rows, make_client, report_query_cost, run_athena_query,
)

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
    with patch('cdx_toolkit.indexes.backends.athena.make_client', return_value=fake), \
            patch('cdx_toolkit.indexes.backends.athena.time.sleep'):
        yield fake


def test_rows_and_cost(client, caplog):
    with caplog.at_level(logging.INFO):
        rows = list(iter_athena_rows('SELECT 1', 'ccindex', 's3://b/p/'))
    assert rows == [dict(zip(COLUMNS, row)) for row in ROWS]
    assert client.started[0]['QueryExecutionContext'] == {'Database': 'ccindex'}
    assert client.started[0]['ResultConfiguration'] == {'OutputLocation': 's3://b/p/'}
    assert 'Athena scanned 2.00 GB, estimated cost ~$0.0100' in caplog.text


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
    with patch('cdx_toolkit.indexes.backends.athena.make_client', return_value=fake), \
            patch('cdx_toolkit.indexes.backends.athena.time.sleep'):
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
    with patch('cdx_toolkit.indexes.backends.athena.time.sleep'), pytest.raises(TimeoutError):
        run_athena_query(fake, 'SELECT 1', 'ccindex', 's3://b/p/', max_wait_time=0)
    assert fake.stopped == ['qid']


def test_cost_report_without_statistics(caplog):
    with caplog.at_level(logging.INFO):
        report_query_cost(FakeAthenaClient(scanned=None), 'qid')
    assert 'Athena scanned' not in caplog.text


def test_make_client_uses_the_region(monkeypatch):
    pytest.importorskip('boto3')
    monkeypatch.delenv('AWS_MAX_ATTEMPTS', raising=False)
    monkeypatch.delenv('AWS_RETRY_MODE', raising=False)
    monkeypatch.setattr('cdx_toolkit.aws.user_aws_config', lambda: {})
    with patch('boto3.client') as boto3_client:
        make_client('eu-west-1')
    args, kwargs = boto3_client.call_args
    assert args == ('athena',) and kwargs['config'].region_name == 'eu-west-1'
    # Common Crawl's recommended retry settings (cdx_toolkit.aws)
    assert kwargs['config'].retries == {'mode': 'adaptive', 'total_max_attempts': 100000}


class _FakeAthenaClient:
    """Minimal stand-in for a boto3 Athena client (no AWS, no network)."""

    def __init__(self, raise_on_wait=None):
        self._raise_on_wait = raise_on_wait
        self.started = []
        self.stopped = []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {'QueryExecutionId': 'qid-123'}

    def get_query_execution(self, QueryExecutionId):
        if self._raise_on_wait is not None:
            raise self._raise_on_wait
        return {'QueryExecution': {'Status': {'State': 'SUCCEEDED'},
                                   'Statistics': {'DataScannedInBytes': 1000}}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)


def test_run_athena_query_logs_sql(caplog):
    import logging
    client = _FakeAthenaClient()
    with caplog.at_level(logging.INFO):
        run_athena_query(client, 'SELECT my_special_column FROM ccindex', 'ccindex', 's3://b/out/')
    assert 'SELECT my_special_column FROM ccindex' in caplog.text


def test_run_athena_query_success_no_cancel():
    client = _FakeAthenaClient()
    qid = run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert qid == 'qid-123'
    assert client.stopped == []


def test_run_athena_query_cancels_on_interrupt():
    client = _FakeAthenaClient(raise_on_wait=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert client.stopped == ['qid-123']


def test_run_athena_query_cancels_on_timeout():
    client = _FakeAthenaClient(raise_on_wait=TimeoutError('too slow'))
    with pytest.raises(TimeoutError):
        run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert client.stopped == ['qid-123']
