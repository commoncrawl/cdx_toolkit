'''
Captures replayed from a wayback machine (e.g. the Internet Archive), as WARC records.
'''
import logging

from cdx_toolkit.repackage.jobs import WaybackJob
from cdx_toolkit.repackage.readers import RangeReader
from cdx_toolkit.repackage.readers.http import run_in_daemon_thread
from cdx_toolkit.repackage.warc_records import get_bytes_from_warc_record
from cdx_toolkit.warc import fetch_wb_warc


logger = logging.getLogger(__name__)


class WaybackReader(RangeReader):
    """Replays a capture with fetch_wb_warc (myrequests_get: retries, and the minimum interval between requests
    to the wayback machine, also across threads) and returns it as a gzipped WARC response record."""

    async def read(self, job: WaybackJob) -> bytes:
        capture = job.capture
        if capture.get('mime') == 'warc/revisit':
            logger.warning('revisit record being resolved for url %s %s', capture['url'], capture['timestamp'])
        # in a thread, so that the event loop (other readers, the writer) is not blocked
        record = await run_in_daemon_thread(fetch_wb_warc, capture, wb=job.wb)
        return get_bytes_from_warc_record(record, gzip=True)
