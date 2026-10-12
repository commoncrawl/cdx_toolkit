from io import BytesIO
import logging
from pathlib import Path
import fsspec
from warcio.recordloader import ArcWarcRecord
from warcio import WARCWriter

from warcio.recordbuilder import RecordBuilder

from typing import Dict, Optional, Tuple, Union

import mimetypes

from cdx_toolkit.repackage.s3_utils import is_s3_url, parse_s3_uri
from cdx_toolkit.repackage.local_writer import LocalFileWriter
from cdx_toolkit.repackage.s3_writer import S3ShardWriter

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


def generate_warc_filename(
    dest_prefix: str,
    sequence: int,
    writer_subprefix: Optional[str] = None,
    gzip: bool = False,
) -> str:
    """Generate a WARC file name from a prefix (can be a full path), an optional subprefix
    (used to distinguish per-process shards), and a sequence index (file-rotation counter)."""
    file_name = dest_prefix + '-'
    if writer_subprefix is not None:
        file_name += writer_subprefix + '-'
    file_name += f'{sequence:03d}.warc'  # TODO default warc command uses ".extracted.warc"
    if gzip:
        file_name += '.gz'

    return file_name


async def create_new_writer_with_header(
    sequence: int,
    output_path_prefix: str,
    max_attempts: int,
    base_backoff_seconds: float,
    min_part_size: int,
    writer_info: Dict,
    warc_version: str = '1.0',
    writer_subprefix: Optional[str] = None,
    gzip: bool = False,
    content_type: Optional[str] = None,
    s3_client=None,
) -> Tuple[Union[S3ShardWriter, LocalFileWriter], int, str]:
    """Create a new WARC writer (local or S3) and write the warcinfo record.

    Returns the writer, the size of the warcinfo record and its WARC-Record-ID."""
    if is_s3_url(output_path_prefix):
        dest_bucket, dest_prefix = parse_s3_uri(output_path_prefix)

        warc_file_path = generate_warc_filename(
            dest_prefix=dest_prefix,
            sequence=sequence,
            writer_subprefix=writer_subprefix,
            gzip=gzip,
        )

        new_writer = S3ShardWriter(
            s3_client,
            warc_file_path,
            dest_bucket,
            content_type,
            min_part_size,
            max_attempts,
            base_backoff_seconds,
        )

    else:
        # local file system
        warc_file_path = generate_warc_filename(
            dest_prefix=output_path_prefix,
            sequence=sequence,
            writer_subprefix=writer_subprefix,
            gzip=gzip,
        )

        new_writer = LocalFileWriter(
            file_path=warc_file_path,
        )

    logger.debug('Initialzing new WARC writer for %s', warc_file_path)

    try:
        return await _start_with_header(new_writer, warc_file_path, writer_info, warc_version, gzip)
    except BaseException:
        await new_writer.abort()
        raise


async def _start_with_header(new_writer, warc_file_path: str, writer_info: Dict, warc_version: str, gzip: bool):
    # Initialize writer
    await new_writer.start()

    # Write WARC header; the `filename` field names this file
    warc_file_name = warc_file_path.split('/')[-1]
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warcinfo = warc_writer.create_warcinfo_record(
        filename=warc_file_name,
        info=writer_info,
    )
    warc_writer.write_record(warcinfo)
    header_data = buffer.getvalue()
    await new_writer.write(header_data)

    # WARC-Warcinfo-ID indicates the WARC-Record-ID of the associated ‘warcinfo’ record
    warcinfo_id = warcinfo.rec_headers.get('WARC-Record-ID')

    return new_writer, len(header_data), warcinfo_id
