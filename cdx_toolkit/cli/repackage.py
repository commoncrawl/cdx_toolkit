from argparse import Namespace, RawDescriptionHelpFormatter
import logging
import os
import time
from typing import Mapping, Optional

import cdx_toolkit.warc
from cdx_toolkit.indexes import UsageError
from cdx_toolkit.record_io import (
    FORMAT_CHOICES, canonical_format, check_glob, format_from_path, import_pyarrow, read_records,
)
from cdx_toolkit.utils import get_version
from cdx_toolkit.warc import fetch_wb_warc

from .common import IncompleteError, add_verbose_arg, parse_size


LOGGER = logging.getLogger(__name__)

# wayback names that map to replay URLs
WAYBACK_NAMES = {'ia': 'https://web.archive.org/web'}

DESCRIPTION = '''\
Fetch the captures listed in an index file (as written by `cdxt index`) and write them to WARC files.

Captures with a WARC location (filename, offset, length; e.g. Common Crawl captures) are copied
byte-for-byte from --warc-download-prefix (https://, s3:// or hf://buckets/...) into
<prefix>[-<subprefix>]-001.warc.gz, -002, ... (local, s3:// or hf://buckets/...).

Captures without a WARC location (e.g. Internet Archive captures) are replayed from a wayback
machine with --wb and written to <prefix>[-<subprefix>]-000000.extracted.warc.gz, ...
'''

EXAMPLES = '''\
examples:
  cdxt index cc --url 'commoncrawl.org/*' --limit 10 -o captures.jsonl
  cdxt repackage captures.jsonl --prefix COMMONCRAWL --size 100MB

  cdxt index cc-url-index-s3 --sql homepages.sql --yes -o homepages.csv
  cdxt repackage homepages.csv --prefix s3://my-bucket/HOMEPAGES --warc-download-prefix s3://commoncrawl \\
      --processes 8 --parallel-readers 16

  cdxt index ia --url 'commoncrawl.org/*' --limit 10 | cdxt repackage --wb ia --prefix IA
'''


def add_parser(subparsers):
    parser = subparsers.add_parser(
        'repackage',
        help='write the captures of an index file to WARC files',
        description=DESCRIPTION,
        epilog=EXAMPLES,
        formatter_class=RawDescriptionHelpFormatter,
    )
    add_verbose_arg(parser, subcommand=True)
    parser.add_argument(
        'input', nargs='?', default='-', metavar='INDEX_PATH',
        help="index file(s) with captures, local or remote, may be a glob (default: '-' = stdin)",
    )
    parser.add_argument(
        '--index-format', choices=('auto',) + FORMAT_CHOICES, default='auto',
        help=('format of the index file (default: parquet for *.parquet, otherwise detected from the first line); '
              "cdx is the same as cdxj: Common Crawl's index files have the .cdx extension but use the CDXJ format "
              '(crawl index JSON)'),
    )

    source = parser.add_argument_group('fetching')
    source.add_argument(
        '--warc-download-prefix', default='https://data.commoncrawl.org', metavar='URL',
        help=('prefix for downloading WARC records by filename (default: %(default)s); s3://commoncrawl is '
              'fastest in AWS us-east-1, hf://buckets/commoncrawl/commoncrawl reads from Hugging Face'),
    )
    source.add_argument(
        '--wb', metavar='URL',
        help="fetch captures by replaying them from this wayback machine ('ia' = Internet Archive)",
    )
    source.add_argument(
        '--processes', type=int, default=1,
        help='number of worker processes, e.g. the number of vCPUs (default: 1)',
    )
    source.add_argument(
        '--parallel-readers', type=int, default=1, metavar='N',
        help='number of concurrent WARC readers per process (default: 1)',
    )

    output = parser.add_argument_group('output')
    output.add_argument(
        '--prefix', default='REPACKAGE',
        help='prefix (path) of the WARC files, local, s3:// or hf://, e.g. out/CC-HOMEPAGES (default: REPACKAGE)'
    )
    output.add_argument('--subprefix', help='subprefix for the WARC filenames')
    output.add_argument(
        '--size', type=parse_size, default=1000000000,
        help='target size of each WARC file, e.g. 1000000000, 100MB or 1GiB (default: 1GB)',
    )
    output.add_argument('--creator', help='creator of the WARC: person, organization, service')
    output.add_argument('--operator', help='a person, if the creator is an organization')
    output.add_argument('--is-part-of', help='isPartOf of the warcinfo record (default: prefix-subprefix)')
    output.add_argument('--description', help='description of the warcinfo record (default: the command line)')
    output.add_argument(
        '--add-metadata-record', action='append', default=[], metavar='PATH',
        help='add the content of this file (local or remote) as a metadata record to each WARC (repeatable)',
    )

    advanced = parser.add_argument_group('advanced')
    advanced.add_argument(
        '--no-sort', action='store_true',
        help=('keep the input order instead of sorting by WARC file and offset (sorting improves read '
              'locality but keeps all captures in memory)'),
    )
    advanced.add_argument(
        '--log-every-n', type=int, default=1000, metavar='N',
        help='log progress every N records (0 = off, default: 1000)',
    )

    parser.set_defaults(func=run)
    return parser


def build_warcinfo(args: Namespace, cmdline: str) -> dict:
    ispartof = args.is_part_of
    if not ispartof:
        ispartof = args.prefix
        if args.subprefix:
            ispartof += '-' + args.subprefix

    info = {
        'software': 'pypi_cdx_toolkit/' + get_version(),
        'isPartOf': ispartof,
        'description': args.description or 'warc extraction generated with: ' + cmdline,
        'format': 'WARC file version 1.0',  # todo: if we directly read a warc, have this match the warc
    }
    if args.creator:
        info['creator'] = args.creator
    if args.operator:
        info['operator'] = args.operator
    return info


def run(args: Namespace, cmdline: str):
    index_format = None if args.index_format == 'auto' else canonical_format(args.index_format)
    try:
        check_glob(args.input)
    except ValueError as e:
        raise UsageError(str(e)) from None
    if index_format == 'parquet' or (index_format is None and format_from_path(args.input) == 'parquet'):
        try:
            import_pyarrow()
        except RuntimeError as e:
            raise UsageError(str(e)) from None
    records = read_records(args.input, index_format)
    info = build_warcinfo(args, cmdline)
    start_time = time.time()

    if args.wb:
        result = repackage_from_wayback(args, records, info)
    else:
        result = repackage_by_warc_location(args, records, info)

    LOGGER.info('wrote %d of %d captures to WARC files (%d failed) in %.1f seconds',
                result.written, result.requested, result.failed, time.time() - start_time)
    if result.failed:
        raise IncompleteError(
            f'{result.failed} of {result.requested} captures could not be fetched (see the log), '
            f'{result.written} were written'
        )


def repackage_by_warc_location(args: Namespace, records, info: dict):
    """Copy the WARC records byte-for-byte with the async WARC fetcher (optionally multi-process)."""
    import fsspec

    from cdx_toolkit.repackage.hf_utils import is_hf_url
    from cdx_toolkit.repackage.sources.records import MissingWarcLocation, RecordsSource

    try:
        source = RecordsSource(records, args.warc_download_prefix, sort=not args.no_sort)

        # make sure the output directory exists
        prefix_fs, prefix_fs_path = fsspec.url_to_fs(args.prefix)
        prefix_fs.makedirs(prefix_fs._parent(prefix_fs_path), exist_ok=True)

        # The WARC writer supports local and S3 output; hf:// output is staged locally by the multi-process mode.
        if args.processes > 1 or is_hf_url(args.prefix):
            from cdx_toolkit.repackage.multiprocess import run_multiprocess_repackage

            return run_multiprocess_repackage(
                source=source,
                n_processes=max(1, args.processes),
                readers_per_process=args.parallel_readers,
                prefix_path=args.prefix,
                writer_info=info,
                writer_subprefix=args.subprefix,
                write_paths_as_metadata_records=args.add_metadata_record,
                log_every_n=args.log_every_n,
                max_attempts=5,
                uvloop=os.environ.get('CDXT_UVLOOP') == '1',
                max_file_size=args.size,
                warc_download_prefix=args.warc_download_prefix,
            )

        from cdx_toolkit.repackage.warc_filter import WARCFilter

        return WARCFilter(
            source=source,
            prefix_path=args.prefix,
            writer_info=info,
            writer_subprefix=args.subprefix,
            write_paths_as_metadata_records=args.add_metadata_record,
            log_every_n=args.log_every_n,
            warc_download_prefix=args.warc_download_prefix,
            n_parallel_readers=args.parallel_readers,
            max_file_size=args.size,
        ).filter()
    except MissingWarcLocation as e:
        raise UsageError(str(e))


def repackage_from_wayback(args: Namespace, records, info: dict):
    """Replay the captures from a wayback machine, one by one, and write them with the WARC writer of cdx_toolkit."""
    for option, used in (
        ('--processes', args.processes > 1),
        ('--parallel-readers', args.parallel_readers > 1),
        ('--add-metadata-record', args.add_metadata_record),
    ):
        if used:
            raise UsageError(f'{option} cannot be used with --wb')

    from cdx_toolkit.repackage.data_classes import RepackageResult

    wb = WAYBACK_NAMES.get(args.wb, args.wb)
    requested = written = 0
    with cdx_toolkit.warc.get_writer(args.prefix, args.subprefix, info, size=args.size) as writer:
        for capture in records:
            requested += 1
            try:
                record = fetch_record(capture, wb)
            except Exception as e:
                LOGGER.error('cannot fetch capture %s %s: %r', capture.get('url'), capture.get('timestamp'), e)
                continue
            writer.write_record(record)
            written += 1
    return RepackageResult(requested=requested, written=written, failed=requested - written)


def fetch_record(capture: Mapping, wb: Optional[str]):
    if capture.get('mime') == 'warc/revisit':
        LOGGER.warning('revisit record being resolved for url %s %s', capture['url'], capture['timestamp'])
    return fetch_wb_warc(capture, wb=wb)
