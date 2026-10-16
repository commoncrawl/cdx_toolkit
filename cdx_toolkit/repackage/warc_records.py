'''
The bytes of the WARC records that cdxt repackage writes itself: warcinfo and metadata records.
'''
from io import BytesIO
import logging
import mimetypes
from pathlib import Path
from typing import Dict, Tuple, Union

import fsspec
from warcio import WARCWriter
from warcio.recordbuilder import RecordBuilder
from warcio.recordloader import ArcWarcRecord

logger = logging.getLogger(__name__)


def get_bytes_from_warc_record(
    record,
    warc_version: str = '1.0',
    gzip: bool = False,
):
    """Get byte representation of WARC record."""
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warc_writer.write_record(record)

    return buffer.getvalue()


def get_metadata_record_from_path(
    file_path: Union[str, Path],
    warcinfo_id: str,
) -> ArcWarcRecord:
    """Build WARC metadata record for file path.

    The `Content-Type` header is guessed.
    """
    # Cast to string
    file_path = str(file_path)

    with fsspec.open(file_path, 'rb') as f:
        file_bytes = BytesIO(f.read())

    warc_content_type = mimetypes.guess_type(file_path)[0]
    warc_headers_dict = {}

    # Set WARC-Warcinfo-ID
    warc_headers_dict['WARC-Warcinfo-ID'] = warcinfo_id

    rb = RecordBuilder()
    record = rb.create_warc_record(
        uri=None,
        record_type='metadata',
        payload=file_bytes,
        http_headers=None,
        warc_content_type=warc_content_type,
        warc_headers_dict=warc_headers_dict,
    )

    return record


def warcinfo_record_bytes(warc_file_name: str, writer_info: Dict, warc_version: str, gzip: bool) -> Tuple[bytes, str]:
    """The warcinfo record of a WARC file (its `filename` field names the file) and its WARC-Record-ID (the
    WARC-Warcinfo-ID of the file's other records)."""
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warcinfo = warc_writer.create_warcinfo_record(filename=warc_file_name, info=writer_info)
    warc_writer.write_record(warcinfo)
    return buffer.getvalue(), warcinfo.rec_headers.get('WARC-Record-ID')
