import pytest

import asyncio


from cdx_toolkit.repackage.s3_utils import (
    _backoff,
    parse_s3_uri,
    with_retries,
)

# the retry tests raise botocore's exceptions (S3 support is optional: cdx_toolkit[s3])
pytest.importorskip('botocore')
from botocore.exceptions import EndpointConnectionError  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402


def test_backoff():
    """Test _backoff function with exponential backoff and jitter."""
    base_backoff = 1.0
    tol_float = 0.001  # tolerance for float errors

    # Test attempt 1: should be between 0.8 and 1.2 seconds (with jitter)
    result1 = _backoff(1, base_backoff)
    assert 0.8 <= result1 <= 1.2 + tol_float

    # Test attempt 2: should be between 1.6 and 2.41 seconds (2^1 * base * jitter)
    result2 = _backoff(2, base_backoff)
    assert 1.6 <= result2 <= 2.4 + tol_float

    # Test attempt 3: should be between 3.2 and 4.8 seconds (2^2 * base * jitter)
    result3 = _backoff(3, base_backoff)
    assert 3.2 <= result3 <= 4.8 + tol_float

    # Test with different base backoff
    base_backoff_small = 0.1
    result_small = _backoff(1, base_backoff_small)
    assert 0.08 <= result_small <= 0.12 + tol_float

    # Test minimum backoff (should never be less than 0.05)
    very_small_base = 0.001
    result_min = _backoff(1, very_small_base)
    assert result_min >= 0.05

    # Test that backoff increases with attempts
    results = [_backoff(i, 0.5) for i in range(1, 6)]
    # Generally should increase, though jitter might cause small variations
    # Check that the trend is generally increasing
    assert results[1] > results[0] * 0.8  # Allow for jitter variation
    assert results[2] > results[1] * 0.8
    assert results[3] > results[2] * 0.8


def test_parse_s3_uri():
    """Test parse_s3_uri function for valid and invalid S3 URIs."""

    # Test valid S3 URIs
    bucket, prefix = parse_s3_uri('s3://my-bucket/path/to/file.txt')
    assert bucket == 'my-bucket'
    assert prefix == 'path/to/file.txt'

    bucket, prefix = parse_s3_uri('s3://test-bucket/folder/subfolder/data.json')
    assert bucket == 'test-bucket'
    assert prefix == 'folder/subfolder/data.json'

    bucket, prefix = parse_s3_uri('s3://simple/file')
    assert bucket == 'simple'
    assert prefix == 'file'

    # Test with deep nested paths
    bucket, prefix = parse_s3_uri('s3://bucket/a/b/c/d/e/f/file.ext')
    assert bucket == 'bucket'
    assert prefix == 'a/b/c/d/e/f/file.ext'

    # Test invalid URIs - should raise ValueError
    with pytest.raises(ValueError, match='Not an S3 URI'):
        parse_s3_uri('http://example.com/path')

    with pytest.raises(ValueError, match='Not an S3 URI'):
        parse_s3_uri('ftp://bucket/file')

    with pytest.raises(ValueError, match='Not an S3 URI'):
        parse_s3_uri('bucket/file')

    # Test malformed S3 URIs
    with pytest.raises(ValueError, match='Malformed S3 URI'):
        parse_s3_uri('s3://')

    with pytest.raises(ValueError, match='Malformed S3 URI'):
        parse_s3_uri('s3://bucket')

    with pytest.raises(ValueError, match='Malformed S3 URI'):
        parse_s3_uri('s3://bucket/')

    with pytest.raises(ValueError, match='Malformed S3 URI'):
        parse_s3_uri('s3:///file')


def test_with_retries_success():
    """Test with_retries function with successful operation on first attempt."""

    async def run_test():
        call_count = 0

        async def successful_coro():
            nonlocal call_count
            call_count += 1
            return 'success'

        result = await with_retries(successful_coro, op_name='test_op', max_attempts=3, base_backoff_seconds=0.1)

        assert result == 'success'
        assert call_count == 1

    asyncio.run(run_test())


def test_with_retries_eventual_success():
    """Test with_retries function that succeeds after initial failures."""

    async def run_test():
        call_count = 0

        async def eventually_successful_coro():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ClientError({'Error': {'Code': 'Throttling'}}, 'test_op')
            return 'success'

        result = await with_retries(
            eventually_successful_coro,
            op_name='test_op',
            max_attempts=3,
            base_backoff_seconds=0.01,  # Very short for testing
        )

        assert result == 'success'
        assert call_count == 3

    asyncio.run(run_test())


def test_with_retries_max_attempts_exceeded():
    """Test with_retries function when max attempts are exceeded."""

    async def run_test():
        call_count = 0

        async def failing_coro():
            nonlocal call_count
            call_count += 1
            raise EndpointConnectionError(endpoint_url='test')

        with pytest.raises(EndpointConnectionError):
            await with_retries(failing_coro, op_name='test_op', max_attempts=2, base_backoff_seconds=0.01)

        assert call_count == 2

    asyncio.run(run_test())


def test_with_retries_non_retryable_exception():
    """Test with_retries function with non-retryable exceptions."""

    async def run_test():
        call_count = 0

        async def failing_coro():
            nonlocal call_count
            call_count += 1
            raise ValueError('Non-retryable error')

        with pytest.raises(ValueError):
            await with_retries(failing_coro, op_name='test_op', max_attempts=3, base_backoff_seconds=0.01)

        # Should fail immediately without retries
        assert call_count == 1

    asyncio.run(run_test())
