import pytest
from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.http_range import RangeError, check_range_response


def test_get_s3_bucket_and_key_from_http_job():
    job = RangeJob(
        url='http://foo.com/example',
        offset=0,
        length=10,
    )
    with pytest.raises(ValueError):
        job.get_s3_bucket_and_key()


@pytest.mark.parametrize('status, content_range, body_length, offset, valid', [
    (206, 'bytes 10-19/100', 10, 10, True),
    (206, 'bytes 10-19/*', 10, 10, True),
    (200, None, 10, 0, True),  # the whole file is the requested range
    (200, None, 100, 10, False),  # range ignored
    (206, 'bytes 11-20/100', 10, 10, False),  # wrong range
    (206, 'bytes 10-19/100', 9, 10, False),  # truncated body
    (206, None, 10, 10, False),
    (302, None, 0, 10, False),  # redirects are not followed
])
def test_check_range_response(status, content_range, body_length, offset, valid):
    def check():
        check_range_response(status, content_range, body_length, offset, 10, 'https://example.org/x')

    if valid:
        check()
    else:
        with pytest.raises(RangeError):
            check()
