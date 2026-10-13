'''
The writers of output files: a local file (local.py) or an S3 object written with a multipart upload (s3.py).
'''
import os
from typing import Optional, Tuple, Union

from cdx_toolkit.repackage.urls import is_s3_url, parse_s3_uri
from cdx_toolkit.repackage.writers.local import LocalFileWriter
from cdx_toolkit.repackage.writers.s3 import S3ShardWriter
from cdx_toolkit.repackage.writers.series import generate_warc_filename


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
