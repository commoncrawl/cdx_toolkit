import asyncio

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.base import RangeJobSource
from cdx_toolkit.repackage.warc_filter import WARCFilter, _STOP


class FakeSource(RangeJobSource):
    def __init__(self, jobs, raise_after=None):
        self.jobs = jobs
        self.raise_after = raise_after

    def iter_range_jobs(self):
        for i, job in enumerate(self.jobs):
            if self.raise_after is not None and i == self.raise_after:
                raise RuntimeError('boom')
            yield job


def _jobs(n):
    return [
        RangeJob(url=f'https://data.commoncrawl.org/{i}.warc.gz', offset=i, length=1, filename=f'{i}.warc.gz')
        for i in range(n)
    ]


def test_producer_emits_stops_even_when_source_raises(tmp_path):
    """Regression: a source that raises mid-iteration must still release the readers."""
    wf = WARCFilter(
        source=FakeSource(_jobs(2), raise_after=1),
        prefix_path=str(tmp_path),
        writer_info={},
        n_parallel_readers=3,
    )

    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        try:
            await wf._produce_range_jobs(queue)
        except RuntimeError:
            pass
        items = []
        while not queue.empty():
            items.append(await queue.get())
        return items

    items = asyncio.run(run())
    stops = sum(1 for it in items if it is _STOP)
    assert wf.num_readers == 3
    assert stops == 3
