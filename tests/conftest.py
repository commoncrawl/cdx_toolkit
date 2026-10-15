"""Shared test fixtures.

By default, the tests make no remote requests:
- the CDX API and HTTP requests (data.commoncrawl.org) are mocked with `responses` and recorded data
  (see mock_response_from_jsonl)
- S3 is a local moto server (see the s3_env fixture and requires_aws_s3)
- Hugging Face reads are faked in the tests that need them

CDXT_LIVE_TESTS is a comma-separated list of the services to test live instead (see LIVE_SERVICES), or `all`:
- http: the CDX API and HTTP requests go to the real servers
- s3: the S3 tests run against AWS, and the tests of DuckDB over s3://commoncrawl run (requires_live_s3)
- athena: the Athena tests run (requires_aws_athena)
- hf: the tests of the Hugging Face Storage Bucket hf://buckets/commoncrawl/commoncrawl run (requires_live_hf)
"""
import gzip
import json
import os
import re
import uuid
import urllib.request
from pathlib import Path
import pytest


try:
    import boto3
    _HAS_BOTO3 = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    boto3 = None
    _HAS_BOTO3 = False

try:
    import botocore.session
    from botocore.config import Config
    from botocore.exceptions import NoCredentialsError, ClientError, EndpointConnectionError
    _HAS_BOTOCORE = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    botocore = None
    Config = None
    NoCredentialsError = ClientError = EndpointConnectionError = Exception
    _HAS_BOTOCORE = False

try:
    import fsspec  # noqa: F401
    _HAS_FSSPEC = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    _HAS_FSSPEC = False

try:
    import duckdb
    _HAS_DUCKDB = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    _HAS_DUCKDB = False

try:
    import huggingface_hub  # noqa: F401
    _HAS_HF = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    _HAS_HF = False

try:
    import aioboto3  # noqa: F401
    _HAS_AIOBOTO3 = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    _HAS_AIOBOTO3 = False

try:
    import moto.server  # noqa: F401
    import s3fs as _s3fs  # noqa: F401
    _HAS_MOTO = _HAS_AIOBOTO3
except ImportError:  # pragma: no cover - exercised in minimal installs
    _HAS_MOTO = False

import functools
from typing import Dict, Optional
import requests
import responses
import base64

from unittest.mock import patch

TEST_DATA_PATH = Path(__file__).parent / 'data'
TEST_DATA_BASE_PATH = Path(__file__).parent / 'data'
TEST_S3_BUCKET = os.environ.get('CDXT_TEST_S3_BUCKET', 'commoncrawl-ci-temp')
TEST_ATHENA_S3_LOCATION = 's3://commoncrawl-ci-temp/athena-results/'
TEST_ATHENA_DATABASE = 'ccindex'
# a writable Hugging Face Storage Bucket (namespace/name) for the hf:// output tests (with CDXT_LIVE_TESTS=hf)
TEST_HF_BUCKET = os.environ.get('CDXT_TEST_HF_BUCKET', 'commoncrawl/commoncrawl-ci-temp')

LIVE_SERVICES = ('http', 's3', 'athena', 'hf')


def _live_services(value: str) -> frozenset:
    """The services of CDXT_LIVE_TESTS (comma-separated, or `all`)."""
    names = {name.strip() for name in value.split(',') if name.strip()}
    if 'all' in names:
        return frozenset(LIVE_SERVICES)
    unknown = names - set(LIVE_SERVICES)
    if unknown:
        raise ValueError(f'CDXT_LIVE_TESTS: unknown service(s) {", ".join(sorted(unknown))}; '
                         f'use a comma-separated list of {", ".join(LIVE_SERVICES)}, or all')
    return frozenset(names)


LIVE_TESTS = _live_services(os.environ.get('CDXT_LIVE_TESTS', ''))

# Cache for AWS S3/Athena access check to avoid repeated network calls
_aws_s3_access_cache = None
_aws_athena_access_cache = None


@pytest.fixture(scope='session', autouse=True)
def cache_dir(tmp_path_factory):
    """An empty cache directory for the tests: the user's cache (CDXT_CACHE_DIR) keeps its list of crawls, and gets
    none of the recorded ones. Also set in the environment, for worker processes."""
    cache = str(tmp_path_factory.mktemp('cdxt-cache')) + '/'
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr('cdx_toolkit.settings.CACHE_DIR', cache)
        mp.setattr('cdx_toolkit.commoncrawl.CACHE_DIR', cache)
        mp.setenv('CDXT_CACHE_DIR', cache)
        yield cache


@pytest.fixture(scope='session', autouse=True)
def set_mock_time():
    """Set CDXT_MOCK_TIME environment variable for consistent test results"""
    # August 15, 2025 - ensures tests use CC-MAIN-2025-33 which exists in mock data
    if 'CDXT_MOCK_TIME' not in os.environ:
        os.environ['CDXT_MOCK_TIME'] = '1755259200'


def check_aws_s3_access():
    """Check if AWS S3 access is available (cached result)."""
    global _aws_s3_access_cache

    if not _HAS_BOTOCORE:
        return False

    if _aws_s3_access_cache is not None:
        return _aws_s3_access_cache

    try:
        config = Config(retries={'max_attempts': 1, 'mode': 'standard'})
        session = botocore.session.Session()
        s3_client = session.create_client('s3', config=config)

        # Try list objects on test bucket
        s3_client.list_objects_v2(Bucket=TEST_S3_BUCKET, MaxKeys=1)
        _aws_s3_access_cache = True
    except (NoCredentialsError, ClientError, ConnectionError, EndpointConnectionError):
        _aws_s3_access_cache = False

    return _aws_s3_access_cache


def requires_live_s3(func):
    """Pytest decorator for tests that need AWS S3 itself (not mockable, e.g. DuckDB over s3://commoncrawl):
    they only run with CDXT_LIVE_TESTS=s3 and AWS S3 access."""
    if 's3' not in LIVE_TESTS:
        return pytest.mark.skip(reason='needs AWS S3; set CDXT_LIVE_TESTS=s3 to run it')(func)
    if not _HAS_BOTOCORE or not _HAS_FSSPEC:
        return pytest.mark.skip(reason='S3 dependencies are not installed; install cdx_toolkit[s3]')(func)
    return pytest.mark.usefixtures('live_aws_s3_access')(func)


@pytest.fixture
def live_aws_s3_access():
    """Skip the test without AWS S3 access; checked when a test needs it (not when the tests are collected)."""
    if not check_aws_s3_access():
        pytest.skip('AWS S3 access not available (no credentials or permissions)')


def requires_aws_s3(func):
    """Pytest decorator for S3 tests: they run against a local moto server (the s3_env fixture), or against
    AWS with CDXT_LIVE_TESTS=s3."""
    if 's3' in LIVE_TESTS:
        return pytest.mark.usefixtures('s3_env')(requires_live_s3(func))
    if not _HAS_MOTO:
        return pytest.mark.skip(reason='moto, s3fs and aioboto3 are needed to mock S3; install cdx_toolkit[all]')(func)
    return pytest.mark.usefixtures('s3_env')(func)


def check_aws_athena_query_execution_access():
    """Check if AWS Athena StartQueryExecution permission is available."""
    try:
        # Use IAM simulation instead of actual query execution
        iam_client = boto3.client('iam')
        response = iam_client.simulate_principal_policy(
            PolicySourceArn=f'arn:aws:sts::{boto3.client("sts").get_caller_identity()["Account"]}:assumed-role/your-role/session',  # noqa: E501
            ActionNames=['athena:StartQueryExecution'],
            ResourceArns=['*'],
        )

        # Check if access is allowed
        return response['EvaluationResults'][0]['EvalDecision'] == 'allowed'

    except (ClientError, NoCredentialsError):
        return False


def check_aws_athena_access():
    """Check if AWS Athena access is available."""
    global _aws_athena_access_cache

    if not _HAS_BOTO3:
        return False

    if _aws_athena_access_cache is not None:
        return _aws_athena_access_cache

    try:
        client = boto3.client('athena')

        # Try list databasets
        client.list_databases(CatalogName='AwsDataCatalog')

        # Try query access
        _aws_athena_access_cache = check_aws_athena_query_execution_access()
    except (NoCredentialsError, ClientError):
        _aws_athena_access_cache = False

    return _aws_athena_access_cache


def requires_aws_athena(func):
    """Pytest decorator that skips test if AWS Athena access is not available (needs CDXT_LIVE_TESTS=athena)."""
    if 'athena' not in LIVE_TESTS:
        return pytest.mark.skip(reason='needs AWS Athena; set CDXT_LIVE_TESTS=athena to run it')(func)
    if not _HAS_BOTO3:
        return pytest.mark.skipif(
            True, reason='S3 dependencies are not installed; install cdx_toolkit[s3] to enable Athena tests.'
        )(func)

    return pytest.mark.usefixtures('live_aws_athena_access')(func)


@pytest.fixture
def live_aws_athena_access():
    """Skip the test without AWS Athena access; checked when a test needs it (not when the tests are collected)."""
    if not check_aws_athena_access():
        pytest.skip('AWS Athena access not available (no credentials or permissions)')


def requires_live_hf(func):
    """Pytest decorator for tests of the Hugging Face Storage Bucket hf://buckets/commoncrawl/commoncrawl: they only
    run with CDXT_LIVE_TESTS=hf (public reads need no token, but have lower rate limits without one)."""
    if 'hf' not in LIVE_TESTS:
        return pytest.mark.skip(reason='needs Hugging Face; set CDXT_LIVE_TESTS=hf to run it')(func)
    if not _HAS_HF:
        return pytest.mark.skip(reason='huggingface_hub is not installed; install cdx_toolkit[hf]')(func)
    return func


def requires_duckdb(func):
    """Pytest decorator that skips a test if the optional duckdb dependency is missing."""
    return pytest.mark.skipif(
        not _HAS_DUCKDB, reason='duckdb is not installed; install cdx_toolkit[duckdb] to enable DuckDB tests.'
    )(func)


def requires_aioboto3(func):
    """Pytest decorator that skips a test if the optional aioboto3 dependency is missing (no AWS access needed)."""
    return pytest.mark.skipif(
        not _HAS_AIOBOTO3, reason='aioboto3 is not installed; install cdx_toolkit[s3] to enable these tests.'
    )(func)


# The WARC records of the first 10 captures of tests/data/repackage/filtered_CC-MAIN-2024-30_cdx-00187.gz,
# recorded from data.commoncrawl.org: {(filename, offset, length): bytes}
WARC_RECORDS = {}
with open(TEST_DATA_PATH / 'repackage' / 'warc_records.jsonl') as _f:
    for _line in _f:
        _r = json.loads(_line)
        WARC_RECORDS[(_r['filename'], _r['offset'], _r['length'])] = base64.b64decode(_r['data'])


def _data_commoncrawl_range(request):
    """responses callback: a range request to data.commoncrawl.org, served from WARC_RECORDS."""
    filename = request.url.split('://', 1)[1].split('/', 1)[1]
    start, end = (int(n) for n in request.headers['Range'][len('bytes='):].split('-'))
    data = WARC_RECORDS.get((filename, start, end - start + 1))
    if data is None:
        return 404, {}, f'no recorded WARC record for {filename} bytes {start}-{end} (see tests/conftest.py)'
    return 206, {'Content-Range': f'bytes {start}-{end}/*'}, data


@pytest.fixture
def mock_data_commoncrawl():
    """WARC record downloads from data.commoncrawl.org are served from recorded data (unless
    CDXT_LIVE_TESTS=http); other requests made with `requests` fail, unless the test adds mock data (e.g.
    with mock_response_from_jsonl)."""
    if 'http' in LIVE_TESTS:
        yield None
        return
    responses.start()
    try:
        responses.add_callback(responses.GET, re.compile(r'https://data\.commoncrawl\.org/.*'),
                               callback=_data_commoncrawl_range)
        yield responses.mock
    finally:
        responses.stop()
        responses.reset()


# crawl and host of the captures of WARC_RECORDS
LOCAL_INDEX_CRAWL = 'CC-MAIN-2024-30'
LOCAL_INDEX_HOST = 'bibliotheque.missiondefrance.fr'


@pytest.fixture
def local_url_index(tmp_path):
    """A columnar URL index like Common Crawl's (<dir>/crawl=.../subset=warc/*.parquet) with the captures of
    WARC_RECORDS, and captures that are not selected by the tests: of another host, and of another crawl."""
    from cdx_toolkit.record_io import parse_cdxj_line

    with gzip.open(TEST_DATA_PATH / 'repackage' / 'filtered_CC-MAIN-2024-30_cdx-00187.gz', 'rt') as f:
        captures = [parse_cdxj_line(f.readline()) for _ in range(len(WARC_RECORDS))]
    rows = {LOCAL_INDEX_CRAWL: captures + [dict(captures[0], url='https://example.com/', urlkey='com,example)/')],
            'CC-MAIN-2024-26': [captures[0]]}

    base = tmp_path / 'url-index'
    con = duckdb.connect()
    for crawl, crawl_rows in rows.items():
        path = base / f'crawl={crawl}' / 'subset=warc'
        path.mkdir(parents=True)
        con.execute('''CREATE OR REPLACE TABLE idx (
            url_surtkey VARCHAR, url VARCHAR, url_host_name VARCHAR, url_host_registered_domain VARCHAR,
            url_host_tld VARCHAR, fetch_time TIMESTAMP, fetch_status SMALLINT, content_mime_type VARCHAR,
            content_mime_detected VARCHAR, content_digest VARCHAR, content_languages VARCHAR, content_charset VARCHAR,
            fetch_redirect VARCHAR, warc_filename VARCHAR, warc_record_offset INTEGER, warc_record_length INTEGER)''')
        for c in crawl_rows:
            host = c['url'].split('/')[2]
            values = "?, ?, ?, ?, ?, strptime(?, '%Y%m%d%H%M%S'), ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?"
            con.execute(f'INSERT INTO idx VALUES ({values})', [
                c['urlkey'], c['url'], host, '.'.join(host.split('.')[-2:]), host.split('.')[-1], c['timestamp'],
                int(c['status']), c['mime'], c['mime-detected'], c['digest'], c['languages'], c.get('charset'),
                c['filename'], int(c['offset']), int(c['length']),
            ])
        con.execute(f"COPY idx TO '{path}/part-00000.parquet' (FORMAT parquet)")
    return base


class InlineExecutor:
    """ProcessPoolExecutor stand-in that runs the workers in this process (mocks apply, coverage is measured)."""

    def __init__(self, max_workers=None, mp_context=None, initializer=None, initargs=()):
        if initializer:
            initializer(*initargs)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def map(self, fn, items):
        return [fn(item) for item in items]


class S3Env:
    """S3 for a test: a local moto server with the buckets commoncrawl (the WARC files of WARC_RECORDS) and
    TEST_S3_BUCKET, or AWS itself (CDXT_LIVE_TESTS=s3)."""

    def __init__(self, offsets=None):
        # (filename, offset) -> offset in the mocked WARC file, which holds only the recorded records
        self.offsets = offsets

    def capture(self, capture):
        """The capture with the offset of its record in s3://commoncrawl (the mocked files are short)."""
        if self.offsets is None:
            return capture
        offset = self.offsets[(capture['filename'], int(capture['offset']))]
        return dict(capture, offset=type(capture['offset'])(offset))


@pytest.fixture(scope='session')
def moto_server():
    import logging
    from moto.server import ThreadedMotoServer

    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    server = ThreadedMotoServer(ip_address='127.0.0.1', port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f'http://{host}:{port}'
    server.stop()


@pytest.fixture
def s3_env(request, monkeypatch, tmp_path):
    """S3 for a test, see S3Env. The moto server is configured with environment variables only, so that also
    worker processes (cdxt repackage --processes) use it."""
    if 's3' in LIVE_TESTS:
        yield S3Env()
        return

    import fsspec.config
    import s3fs
    from cdx_toolkit import aws

    endpoint = request.getfixturevalue('moto_server')
    # path-style addressing: the endpoint is an IP address (an explicit S3 setting of the AWS config file wins
    # over cdxt's defaults)
    config_file = tmp_path / 'aws-config'
    config_file.write_text('[default]\nregion = us-east-1\ns3 =\n  addressing_style = path\n')
    for name in ('AWS_PROFILE', 'AWS_SESSION_TOKEN', 'AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_MAX_ATTEMPTS',
                 'AWS_RETRY_MODE', 'CDXT_AWS_DEFAULTS'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('AWS_ENDPOINT_URL_S3', endpoint)
    monkeypatch.setenv('AWS_CONFIG_FILE', str(config_file))
    monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(tmp_path / 'aws-credentials'))
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setitem(fsspec.config.conf, 's3', {})
    aws.user_aws_config.cache_clear()
    s3fs.S3FileSystem.clear_instance_cache()

    urllib.request.urlopen(urllib.request.Request(f'{endpoint}/moto-api/reset', method='POST'))
    client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1')
    client.create_bucket(Bucket='commoncrawl')
    client.create_bucket(Bucket=TEST_S3_BUCKET)
    offsets, files = {}, {}
    for (filename, offset, _), data in sorted(WARC_RECORDS.items()):
        offsets[(filename, offset)] = len(files.get(filename, b''))
        files[filename] = files.get(filename, b'') + data
    for filename, data in files.items():
        client.put_object(Bucket='commoncrawl', Key=filename, Body=data)

    yield S3Env(offsets)

    aws.user_aws_config.cache_clear()
    s3fs.S3FileSystem.clear_instance_cache()


@pytest.fixture
def s3_tmpdir(s3_env):
    """S3 equivalent of tmpdir - provides a temporary S3 path and handles cleanup."""
    bucket_name = TEST_S3_BUCKET

    # Generate unique prefix using UUID to avoid collisions
    temp_prefix = f'cdx_toolkit/ci/tmpdirs/{uuid.uuid4().hex}'

    # Yield the S3 path
    yield f's3://{bucket_name}/{temp_prefix}'

    try:
        # Cleanup: delete all objects with this prefix
        s3_client = boto3.client('s3')

        # List all objects with the temp prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=temp_prefix)

        if 'Contents' in response:
            # Delete all objects
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
    except (NoCredentialsError, ClientError, ConnectionError, EndpointConnectionError):
        # Ignore cleanup errors - test objects will eventually expire
        pass


@pytest.fixture
def hf_tmpdir():
    """A temporary path in the Hugging Face Storage Bucket TEST_HF_BUCKET (CDXT_LIVE_TESTS=hf), removed afterwards."""
    from huggingface_hub import HfFileSystem

    path = f'buckets/{TEST_HF_BUCKET}/cdx_toolkit/ci/tmpdirs/{uuid.uuid4().hex}'
    yield f'hf://{path}'
    try:
        HfFileSystem().rm(path, recursive=True)
    except Exception:  # noqa: BLE001 - cleanup only; nothing written, or no permission
        pass


def flexible_param_matcher(expected_params):
    """Custom matcher that ignores dynamic 'from' parameter timestamps and casts all values to strings"""

    def match(request):
        actual_params = dict(request.params or {})
        expected = dict(expected_params or {})

        # Remove 'from' parameter for comparison as it's dynamically generated
        actual_params.pop('from', None)
        expected.pop('from', None)

        # Cast all values to strings for consistent comparison
        # Handle list values specially - if expected is a list with one item, match against the string value
        actual_params_normalized = {}
        expected_normalized = {}

        for k, v in actual_params.items():
            if v is not None:
                actual_params_normalized[k] = str(v)

        for k, v in expected.items():
            # Skip parameters with None values - they represent optional parameters
            if v is None or str(v) == 'None':
                continue
            if isinstance(v, list) and len(v) == 1:
                expected_normalized[k] = str(v[0])
            else:
                expected_normalized[k] = str(v)

        if actual_params_normalized == expected_normalized:
            return True, "Params match (ignoring 'from' parameter)"
        else:
            return (
                False,
                f"Params don't match: {actual_params_normalized} != {expected_normalized}",
            )

    return match


def mock_response_from_jsonl(mock_data_name, mock_data_dir: Optional[str] = None):
    """Load mock response data from JSONL file. Response match based on URL and request params.

    By default, all remote requests are mocked. To change this behaviour and actually call remote APIs
    (if you run this from a whitelisted IP address), the following environment variable can be set:

    ```bash
    export CDXT_LIVE_TESTS=http
    ```

    If the remote APIs change, new mock data can be semi-automatically collected
    by setting another environment variable, running corresponding unit tests,
    and overwriting existing mock data in `tests/data/mock_responses`:

    ```bash
    # set environment variable (CDXT_LIVE_TESTS should not have http)
    export CDXT_SAVE_MOCK_RESPONSES=./tmp/mock_responses

    # run the test for what mock data should be saved to $CDXT_SAVE_MOCK_RESPONSES/<test_file>/<test_func>.jsonl
    pytest tests/test_cli.py::test_basics
    ```

    Make sure to empty the cache before collecting mock data (~/.cache/cdx_toolkit/).

    The mock data can then be stored as fixture file in "tests/data/mock_responses/<test module>/<test func>.jsonl".
    """
    mock_data_file_path = TEST_DATA_BASE_PATH / 'mock_responses'

    if mock_data_dir:
        mock_data_file_path = mock_data_file_path / mock_data_dir

    mock_data_file_path = mock_data_file_path / f'{mock_data_name}.jsonl'

    # Read JSONL file
    with open(mock_data_file_path) as f:
        for line in f:
            if line:  # skip empty lines
                mock_data = json.loads(line)

                if mock_data['url'].endswith('.gz'):
                    # decode base64 string to gzipped bytes
                    body = base64.b64decode(mock_data['response_text'].encode('utf-8'))
                else:
                    body = mock_data['response_text']

                headers = mock_data.get('response_headers', {})

                # Remove encoding header
                if 'Content-Encoding' in headers:
                    del headers['Content-Encoding']

                responses.add(
                    mock_data['method'],
                    mock_data['url'],
                    status=mock_data['response_status_code'],
                    match=[flexible_param_matcher(mock_data['request_params'])],
                    body=body,
                    headers=headers,
                )


def conditional_mock_responses(func=None, *, auto_mock_data: bool = True):
    """Applies @responses.activate and auto-loads mock data, unless CDXT_LIVE_TESTS has http.

    The mock data is automatically loaded from JSONL file from the tests/data directory
    and dependinng on the test module and test function.

    Args:
        auto_mock_data: If True, auto-loads test-specific mock data. If False, only loads CC endpoints.
    """

    def decorator(f):
        # without CDXT_LIVE_TESTS=http, the responses are mocked
        if 'http' not in LIVE_TESTS:
            # Add responses.activate
            f = add_mock_responses(f, auto_mock_data=auto_mock_data)

        if os.environ.get('CDXT_SAVE_MOCK_RESPONSES'):
            # Mock data is saved by capturing output from requests.get
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                with patch('requests.get', side_effect=_custom_behavior_with_original(requests.get)):
                    return f(*args, **kwargs)

            return wrapper

        return f

    if func is None:
        # Called with arguments: @conditional_mock_responses(auto_mock_data=False)
        return decorator
    else:
        # Called without arguments: @conditional_mock_responses
        return decorator(func)


def save_response_as_mock_data(test_info: str, request_url: str, request_params: Dict, resp, output_base_dir: str):
    """Save request and response for mock data."""
    # Format: "path/to/test_file.py::TestClass::test_method (setup)"
    test_file = Path(test_info.split('::')[0]).stem
    test_name = test_info.split('::')[-1].split()[0]

    output_dir = os.path.join(output_base_dir, test_file)

    # Make sure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Append to JSONL file
    with open(os.path.join(output_dir, f'{test_name}.jsonl'), 'a') as f:
        if request_url.endswith('.gz'):
            # encode bytes as base64 string if file is gzipped
            response_text = base64.b64encode(resp.content).decode('utf-8')
        else:
            response_text = resp.text

        f.write(
            json.dumps(
                {
                    'method': 'GET',
                    'url': request_url,
                    'request_params': request_params,
                    'response_status_code': resp.status_code,
                    'response_text': response_text,
                    'response_headers': dict(resp.headers),
                }
            )
            + '\n'
        )


def _custom_behavior_with_original(original_func):
    def custom_behavior(*args, **kwargs):
        # Call the original "requests.get"
        response = original_func(*args, **kwargs)

        if 'url' in kwargs:
            request_url = kwargs.pop('url')
        else:
            request_url = args[0]

        if 'params' in kwargs:
            request_params = kwargs.pop('params')
        else:
            request_params = args[1]

        # Make sure this is run as a unit test
        if os.environ.get('PYTEST_CURRENT_TEST'):
            save_response_as_mock_data(
                test_info=os.environ['PYTEST_CURRENT_TEST'],
                request_url=request_url,
                request_params=request_params,
                resp=response,
                output_base_dir=os.environ.get('CDXT_SAVE_MOCK_RESPONSES'),
            )

        return response

    return custom_behavior


def add_mock_responses(func, auto_mock_data: bool = True):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Load mock data for index calls (same for many test functions)
        mock_response_from_jsonl('test_get_cc_endpoints', 'test_cc')

        # Auto-load mock data based on function name
        if auto_mock_data:
            mock_response_from_jsonl(func.__name__, func.__module__.split('.')[-1])
        return func(*args, **kwargs)

    return responses.activate(wrapper)
