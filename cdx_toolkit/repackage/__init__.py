'''
Write captures to WARC files: copy their WARC records byte-for-byte, or replay them from a wayback machine.

The Python equivalent of `cdxt repackage`:

    from cdx_toolkit.indexes import IndexQuery, open_index
    from cdx_toolkit.repackage import repackage

    captures = open_index('cc').records(IndexQuery(urls='commoncrawl.org/*', limit=10))
    result = repackage(captures, prefix='out/COMMONCRAWL')
'''
import logging
import os
from typing import Iterable, Mapping, Optional, Sequence, Union

from cdx_toolkit.indexes import UsageError
from cdx_toolkit.repackage.jobs import RepackageResult


logger = logging.getLogger(__name__)


def repackage(
    captures: Union[str, Iterable[Mapping]],
    prefix: str = 'REPACKAGE',
    *,
    subprefix: Optional[str] = None,
    size: int = 1_000_000_000,
    warc_download_prefix: str = 'https://data.commoncrawl.org',
    wb: Optional[str] = None,
    processes: int = 1,
    parallel_readers: int = 1,
    metadata_records: Sequence[str] = (),
    sort: bool = True,
    log_every_n: int = 1000,
    creator: Optional[str] = None,
    operator: Optional[str] = None,
    is_part_of: Optional[str] = None,
    description: Optional[str] = None,
    index_format: Optional[str] = None,
) -> RepackageResult:
    """Write the captures to the WARC files <prefix>[-<subprefix>]-001.warc.gz, -002, ... (local, s3:// or hf://).

    Args:
        captures: index records (e.g. IndexBackend.records()), or the path of an index file as written by
            `cdxt index` (local or remote, may be a glob; '-' = stdin).
        prefix, subprefix: the output files; files of this prefix and subprefix must not exist yet.
        size: target byte size of each WARC file.
        warc_download_prefix: where the WARC files of the captures (filename, offset, length) are read from:
            https://, s3:// or hf://buckets/...
        wb: replay the captures from this wayback machine instead ('ia' = the Internet Archive); for captures
            without a WARC location.
        processes, parallel_readers: worker processes, and concurrent readers per process.
        metadata_records: files (local or remote) added as a metadata record to each WARC file.
        sort: sort the captures by WARC file and offset (better read locality, keeps the captures in memory).
        log_every_n: log the progress every N records (0 = off).
        creator, operator, is_part_of, description: fields of the warcinfo record.
        index_format: format of the index file, if `captures` is a path (default: detected).

    Returns the number of captures requested, written, and failed (could not be fetched; the others are
    written). Raises UsageError for invalid options, and if the output cannot be written."""
    if wb and processes > 1:
        # the minimum interval between requests to the wayback machine only holds within a process
        raise UsageError('--processes cannot be used with --wb')
    if isinstance(captures, str):
        captures = read_index_file(captures, index_format)
    check_output_is_new(prefix, subprefix)
    info = warcinfo(prefix, subprefix, creator, operator, is_part_of, description)
    return _run(captures, prefix, subprefix, size, warc_download_prefix, wb, processes, parallel_readers,
                list(metadata_records), sort, log_every_n, info)


def read_index_file(path: str, index_format: Optional[str] = None) -> Iterable[dict]:
    from cdx_toolkit.record_io import canonical_format, check_glob, format_from_path, import_pyarrow, read_records

    index_format = None if index_format in (None, 'auto') else canonical_format(index_format)
    try:
        check_glob(path)
    except ValueError as e:
        raise UsageError(str(e)) from None
    if index_format == 'parquet' or (index_format is None and format_from_path(path) == 'parquet'):
        try:
            import_pyarrow()
        except RuntimeError as e:
            raise UsageError(str(e)) from None
    return read_records(path, index_format)


def check_output_is_new(prefix: str, subprefix: Optional[str]) -> None:
    """Every run numbers its files from the start: never overwrite (or mix with) the files of an earlier run."""
    from cdx_toolkit.repackage.writers.series import existing_warc_files

    existing = existing_warc_files(prefix, subprefix)
    if existing:
        raise UsageError(
            f'{len(existing)} WARC file(s) with this --prefix/--subprefix exist already, e.g. {existing[0]}; '
            'use another prefix or subprefix, or remove them'
        )


def warcinfo(prefix: str, subprefix: Optional[str] = None, creator: Optional[str] = None,
             operator: Optional[str] = None, is_part_of: Optional[str] = None,
             description: Optional[str] = None) -> dict:
    """The fields of the warcinfo record at the start of each WARC file."""
    from cdx_toolkit.utils import get_version

    if not is_part_of:
        is_part_of = prefix + ('-' + subprefix if subprefix else '')
    info = {
        'software': 'pypi_cdx_toolkit/' + get_version(),
        'isPartOf': is_part_of,
        'description': description or 'warc extraction generated with cdx_toolkit',
        'format': 'WARC file version 1.0',  # todo: if we directly read a warc, have this match the warc
    }
    if creator:
        info['creator'] = creator
    if operator:
        info['operator'] = operator
    return info


def _run(captures, prefix, subprefix, size, warc_download_prefix, wb, processes, parallel_readers,
         metadata_records, sort, log_every_n, info) -> RepackageResult:
    """Copy the WARC records byte-for-byte, or replay them from a wayback machine, with the async repackager
    (optionally multi-process)."""
    import fsspec

    from cdx_toolkit.repackage.jobs import MissingWarcLocation, RecordsSource, WaybackSource
    from cdx_toolkit.repackage.urls import is_hf_url
    try:
        if wb:
            source = WaybackSource(captures, wb)
        else:
            source = RecordsSource(captures, warc_download_prefix, sort=sort)
    except MissingWarcLocation as e:
        raise UsageError(str(e)) from None

    # make sure the output directory exists
    prefix_fs, prefix_fs_path = fsspec.url_to_fs(prefix)
    prefix_fs.makedirs(prefix_fs._parent(prefix_fs_path), exist_ok=True)

    try:
        # The WARC writer supports local and S3 output; hf:// output is staged locally by the multi-process mode.
        if processes > 1 or is_hf_url(prefix):
            from cdx_toolkit.repackage.multiprocess import run_multiprocess_repackage

            return run_multiprocess_repackage(
                source=source,
                n_processes=processes,
                readers_per_process=parallel_readers,
                prefix_path=prefix,
                writer_info=info,
                writer_subprefix=subprefix,
                write_paths_as_metadata_records=metadata_records,
                log_every_n=log_every_n,
                max_attempts=5,
                uvloop=os.environ.get('CDXT_UVLOOP') == '1',
                max_file_size=size,
            )

        from cdx_toolkit.repackage.repackager import Repackager

        return Repackager(
            source=source,
            prefix_path=prefix,
            writer_info=info,
            writer_subprefix=subprefix,
            write_paths_as_metadata_records=metadata_records,
            log_every_n=log_every_n,
            n_parallel_readers=parallel_readers,
            max_file_size=size,
        ).run()
    except MissingWarcLocation as e:  # with sort=False, the records are read while the jobs are run
        raise UsageError(str(e)) from None
