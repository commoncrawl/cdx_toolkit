from tests.conftest import TEST_DATA_PATH

from cdx_toolkit.repackage.pipeline import ThroughputTracker, WARCFilter
from cdx_toolkit.repackage.jobs import RecordsSource

fixture_path = TEST_DATA_PATH / 'repackage'

# A throwaway source for unit tests that only exercise reader/writer/rotate/log methods.
_FAKE_SOURCE = RecordsSource([], 'https://data.commoncrawl.org')


def test_log_writer(caplog):
    """Test log writer."""
    import logging
    caplog.set_level(logging.INFO, logger='cdx_toolkit.repackage.pipeline')

    warc_filter = WARCFilter(
        source=_FAKE_SOURCE,
        prefix_path='/fake/prefix',
        writer_info={'writer_id': 1},
        log_every_n=2,
    )
    tracker = ThroughputTracker()
    warc_filter.log_writer(0, tracker)
    warc_filter.log_writer(1, tracker)
    warc_filter.log_writer(2, tracker)

    assert caplog.text.count('WARC Writer') == 2


def test_log_reader(caplog):
    """Test log reader."""
    import logging
    caplog.set_level(logging.INFO, logger='cdx_toolkit.repackage.pipeline')

    warc_filter = WARCFilter(
        source=_FAKE_SOURCE,
        prefix_path='/fake/prefix',
        writer_info={'writer_id': 1},
        log_every_n=2,
    )
    tracker = ThroughputTracker()
    warc_filter.log_reader(1, 0, tracker)
    warc_filter.log_reader(1, 1, tracker)
    warc_filter.log_reader(1, 2, tracker)

    assert caplog.text.count('WARC Reader 1') == 2
