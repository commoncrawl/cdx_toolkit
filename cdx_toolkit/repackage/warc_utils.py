from io import BytesIO
import glob
import logging
import os
import posixpath
import re
from pathlib import Path
import fsspec
from warcio.recordloader import ArcWarcRecord
from warcio import WARCWriter

from warcio.recordbuilder import RecordBuilder

from typing import Dict, List, Optional, Tuple, Union

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


def existing_warc_files(prefix_path: str, writer_subprefix: Optional[str] = None, gzip: bool = True) -> List[str]:
    """The files of the series <prefix>[-<subprefix>]-NNN.warc[.gz] (async WARC fetcher) or
    <prefix>[-<subprefix>]-NNNNNN.extracted.warc[.gz] (--wb) that already exist (local, s3://, hf://, ...)."""
    fs, fs_prefix = fsspec.url_to_fs(prefix_path)
    name = re.escape(posixpath.basename(fs_prefix)) + '-'
    if writer_subprefix is not None:
        name += re.escape(writer_subprefix) + '-'
    name += r'\d{3,}(\.extracted)?\.warc' + (r'\.gz' if gzip else '')
    return sorted(
        fs.unstrip_protocol(path) for path in fs.glob(glob.escape(fs_prefix) + '-*')
        if re.fullmatch(name, posixpath.basename(path))
    )


def new_warc_writer(
    sequence: int,
    output_path_prefix: str,
    max_attempts: int,
    base_backoff_seconds: float,
    min_part_size: int,
    writer_subprefix: Optional[str] = None,
    gzip: bool = False,
    s3_client=None,
) -> Tuple[Union[S3ShardWriter, LocalFileWriter], str]:
    """A new (not started) writer of a WARC file, local or S3, and the file's name (for the warcinfo record).

    Both writers have the same life cycle: start(), write()..., then close(), which completes the file, or
    abort(), which removes it (or aborts its upload); a writer that cannot close aborts itself."""
    if is_s3_url(output_path_prefix):
        dest_bucket, dest_prefix = parse_s3_uri(output_path_prefix)
        key = generate_warc_filename(dest_prefix, sequence=sequence, writer_subprefix=writer_subprefix, gzip=gzip)
        writer = S3ShardWriter(s3_client, key, dest_bucket, min_part_size, max_attempts, base_backoff_seconds)
        return writer, key.split('/')[-1]

    path = generate_warc_filename(output_path_prefix, sequence=sequence, writer_subprefix=writer_subprefix, gzip=gzip)
    return LocalFileWriter(file_path=path), os.path.basename(path)  # with backslashes on Windows


def warcinfo_record_bytes(warc_file_name: str, writer_info: Dict, warc_version: str, gzip: bool) -> Tuple[bytes, str]:
    """The warcinfo record of a WARC file (its `filename` field names the file) and its WARC-Record-ID (the
    WARC-Warcinfo-ID of the file's other records)."""
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warcinfo = warc_writer.create_warcinfo_record(filename=warc_file_name, info=writer_info)
    warc_writer.write_record(warcinfo)
    return buffer.getvalue(), warcinfo.rec_headers.get('WARC-Record-ID')
