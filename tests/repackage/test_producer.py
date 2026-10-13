import glob
from unittest.mock import patch

import pytest

from cdx_toolkit.repackage.jobs import RangeJob
from cdx_toolkit.repackage.repackager import Repackager
from cdx_toolkit.repackage.readers.http import HttpRangeReader


class FailingSource:
    def iter_range_jobs(self):
        yield RangeJob(url='https://data.commoncrawl.org/0.warc.gz', offset=0, length=1)
        raise RuntimeError('boom')


def test_failing_source_fails_the_pipeline(tmp_path):
    """A source that raises mid-iteration stops the readers and the writer, the error propagates."""
    async def read(self, job):
        return b'x'

    repackager = Repackager(
        source=FailingSource(), prefix_path=str(tmp_path / 'out'), writer_info={}, n_parallel_readers=3,
    )
    with patch.object(HttpRangeReader, 'read', read), pytest.raises(RuntimeError, match='boom'):
        repackager.run()
    assert glob.glob(str(tmp_path / 'out-*')) == []
