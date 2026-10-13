from tests.conftest import TEST_DATA_PATH

from cdx_toolkit.repackage.pipeline import ThroughputTracker, WARCFilter
from cdx_toolkit.repackage.jobs import RecordsSource

fixture_path = TEST_DATA_PATH / 'repackage'

# A throwaway source for unit tests that only exercise reader/writer/rotate/log methods.
_FAKE_SOURCE = RecordsSource([], 'https://data.commoncrawl.org')


def test_log_progress(caplog):
    """Progress is logged every --log-every-n items."""
    import logging
    caplog.set_level(logging.INFO, logger='cdx_toolkit.repackage.pipeline')

    warc_filter = WARCFilter(
        source=_FAKE_SOURCE,
        prefix_path='/fake/prefix',
        writer_info={'writer_id': 1},
        log_every_n=2,
    )
    tracker = ThroughputTracker()
    for _ in range(5):
        tracker.add(bytes_count=1024 * 1024, records_count=1)
        warc_filter.log_progress('WARC Reader 1', tracker)

    assert caplog.text.count('WARC Reader 1: ') == 2
    assert 'WARC Reader 1: 4 items, 4.0 MB in ' in caplog.text
