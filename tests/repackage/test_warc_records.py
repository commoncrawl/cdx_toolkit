import fsspec
import pytest

from cdx_toolkit.repackage.warc_records import get_metadata_record_from_path
from cdx_toolkit.repackage.writers.series import existing_warc_files
from tests.conftest import TEST_DATA_PATH


def test_get_metadata_record_from_path():
    file_path = TEST_DATA_PATH / 'repackage/url_list.txt'
    record = get_metadata_record_from_path(file_path, warcinfo_id="abc123")

    assert record.content_type == 'text/plain'

    record_headers = dict(record.rec_headers.headers)

    assert record_headers["WARC-Warcinfo-ID"] == "abc123", "Invalid Warcinfo-ID"
    assert record_headers["WARC-Payload-Digest"] == "sha1:VXA2A5YUS3TAY36AUO6MACRMNOH5RXG2", "Invalid payload digest"


@pytest.mark.parametrize('subprefix, expected', [
    (None, ['out-000000.extracted.warc.gz', 'out-001.warc.gz', 'out-1000.warc.gz']),
    ('SUB', ['out-SUB-002.warc.gz']),
    ('OTHER', []),
])
def test_existing_warc_files(tmp_path, subprefix, expected):
    for name in ['out-001.warc.gz', 'out-1000.warc.gz', 'out-000000.extracted.warc.gz', 'out-x.extracted.warc.gz',
                 'out-SUB-002.warc.gz', 'out-x.warc.gz', 'out-001.warc', 'outer-001.warc.gz', 'other-001.warc.gz']:
        (tmp_path / name).write_bytes(b'')
    found = existing_warc_files(str(tmp_path / 'out'), subprefix)
    assert [f.rsplit('/', 1)[-1] for f in found] == expected


def test_existing_warc_files_remote_and_missing_dir(tmp_path):
    with fsspec.open('memory://existing/b[1]/out-003.warc.gz', 'wb') as f:
        f.write(b'')
    found, = existing_warc_files('memory://existing/b[1]/out')
    assert found.endswith('existing/b[1]/out-003.warc.gz')
    assert existing_warc_files(str(tmp_path / 'missing' / 'out')) == []
