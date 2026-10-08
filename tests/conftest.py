import json
import os
import uuid
from pathlib import Path
import pytest

from cdx_toolkit.settings import CACHE_DIR

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
    import duckdb  # noqa: F401
    _HAS_DUCKDB = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    _HAS_DUCKDB = False

import functools
from typing import Dict, Optional
import requests
import responses
import base64
import shutil

from unittest.mock import patch

TEST_DATA_PATH = Path(__file__).parent / 'data'
TEST_DATA_BASE_PATH = Path(__file__).parent / 'data'
TEST_S3_BUCKET = os.environ.get('CDXT_TEST_S3_BUCKET', 'commoncrawl-ci-temp')
TEST_ATHENA_S3_LOCATION = 's3://commoncrawl-ci-temp/athena-results/'
TEST_ATHENA_DATABASE = 'ccindex'
DISABLE_ATHENA_TESTS = bool(os.environ.get('CDXT_DISABLE_ATHENA_TESTS', False))
DISABLE_S3_TESTS = bool(os.environ.get('CDXT_DISABLE_S3_TESTS', False))

# Cache for AWS S3/Athena access check to avoid repeated network calls
_aws_s3_access_cache = None
_aws_athena_access_cache = None


@pytest.fixture(scope='session', autouse=True)
def cleanup_cache():
    """Delete cache directory before each test to ensure clean state"""
    cache_dir = os.path.expanduser(CACHE_DIR)
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)


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


def requires_aws_s3(func):
    """Pytest decorator that skips test if AWS S3 access is not available or disabled."""
    if not _HAS_BOTOCORE or not _HAS_FSSPEC:
        return pytest.mark.skipif(
            True, reason='S3 dependencies are not installed; install warcio[s3] to enable S3 tests.'
        )(func)

    return pytest.mark.skipif(DISABLE_S3_TESTS, reason='AWS S3 access is disabled via environment variable.')(
        pytest.mark.skipif(
            not check_aws_s3_access(), reason='AWS S3 access not available (no credentials or permissions)'
        )(func)
    )


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
    """Pytest decorator that skips test if AWS Athena access is not available."""
    if not _HAS_BOTO3:
        return pytest.mark.skipif(
            True, reason='S3 dependencies are not installed; install cdx_toolkit[s3] to enable Athena tests.'
        )(func)

    return pytest.mark.skipif(DISABLE_ATHENA_TESTS, reason='AWS Athena access is disabled via environment variable.')(
        pytest.mark.skipif(
            not check_aws_athena_access(), reason='AWS Athena access not available (no credentials or permissions)'
        )(func)
    )


def requires_duckdb(func):
    """Pytest decorator that skips a test if the optional duckdb dependency is missing."""
    return pytest.mark.skipif(
        not _HAS_DUCKDB, reason='duckdb is not installed; install cdx_toolkit[duckdb] to enable DuckDB tests.'
    )(func)


@pytest.fixture
def s3_tmpdir():
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
    export DISABLE_MOCK_RESPONSES=1
    ```

    If the remote APIs change, new mock data can be semi-automatically collected
    by setting another environment variable, running corresponding unit tests,
    and overwriting existing mock data in `tests/data/mock_responses`:

    ```bash
    # set environment variable (DISABLE_MOCK_RESPONSES should not be set)
    export SAVE_MOCK_RESPONSES=./tmp/mock_responses

    # run the test for what mock data should be saved to $SAVE_MOCK_RESPONSES/<test_file>/<test_func>.jsonl
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
    """Conditionally applies @responses.activate and auto-loads mock data based on DISABLE_MOCK_RESPONSES env var.

    The mock data is automatically loaded from JSONL file from the tests/data directory
    and dependinng on the test module and test function.

    Args:
        auto_mock_data: If True, auto-loads test-specific mock data. If False, only loads CC endpoints.
    """

    def decorator(f):
        # If the flag DISABLE_MOCK_RESPONSES is not detected, response mocking remains enabled
        if not os.environ.get('DISABLE_MOCK_RESPONSES'):
            # Add responses.activate
            f = add_mock_responses(f, auto_mock_data=auto_mock_data)

        if os.environ.get('SAVE_MOCK_RESPONSES'):
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
                output_base_dir=os.environ.get('SAVE_MOCK_RESPONSES'),
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
