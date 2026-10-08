import pytest
from cdx_toolkit.repackage.data_classes import RangeJob


def test_get_s3_bucket_and_key_from_http_job():
    job = RangeJob(
        url='http://foo.com/example',
        offset=0,
        length=10,
    )
    with pytest.raises(ValueError):
        job.get_s3_bucket_and_key()
