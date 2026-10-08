from cdx_toolkit.repackage.warc_utils import get_metadata_record_from_path
from tests.conftest import TEST_DATA_PATH


def test_get_metadata_record_from_path():
    file_path = TEST_DATA_PATH / 'repackage/url_list.txt'
    record = get_metadata_record_from_path(file_path, warcinfo_id="abc123")

    assert record.content_type == 'text/plain'

    record_headers = dict(record.rec_headers.headers)

    assert record_headers["WARC-Warcinfo-ID"] == "abc123", "Invalid Warcinfo-ID"
    assert record_headers["WARC-Payload-Digest"] == "sha1:VXA2A5YUS3TAY36AUO6MACRMNOH5RXG2", "Invalid payload digest"
