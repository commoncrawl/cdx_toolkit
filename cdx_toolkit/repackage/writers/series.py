'''
The output of a run: the series of WARC files <prefix>[-<subprefix>]-NNN.warc.gz.
'''
import glob
import posixpath
import re
from typing import List, Optional

import fsspec


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
