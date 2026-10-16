from argparse import Namespace, RawDescriptionHelpFormatter
import logging
import time

from cdx_toolkit.record_io import FORMAT_CHOICES
from cdx_toolkit.repackage import repackage

from .common import IncompleteError, add_verbose_arg, parse_size, positive_int


LOGGER = logging.getLogger(__name__)

DESCRIPTION = '''\
Fetch the captures listed in an index file (as written by `cdxt index`) and write them to WARC files.

Captures with a WARC location (filename, offset, length; e.g. Common Crawl captures) are copied
byte-for-byte from --warc-download-prefix (https://, s3:// or hf://buckets/...) into
<prefix>[-<subprefix>]-001.warc.gz, -002, ... (local, s3:// or hf://buckets/...).

Captures without a WARC location (e.g. Internet Archive captures) are replayed from a wayback
machine with --wb and written to the same series of WARC files.

WARC files with the same prefix and subprefix must not exist yet.
'''

EXAMPLES = '''\
examples:
  cdxt index cc --url 'commoncrawl.org/*' --limit 10 -o captures.jsonl
  cdxt repackage captures.jsonl --prefix COMMONCRAWL --size 100MB

  cdxt index cc-url-index-s3 --sql examples/homepages.sql --yes -o homepages.csv  # in the cdx_toolkit repo
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
        '--input-format', choices=('auto',) + FORMAT_CHOICES, default='auto',
        help=('format of the input file (default: parquet for *.parquet, otherwise detected from the first line); '
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
        '--processes', type=positive_int, default=1,
        help='number of worker processes, e.g. the number of vCPUs (default: 1)',
    )
    source.add_argument(
        '--parallel-readers', type=positive_int, default=1, metavar='N',
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


def run(args: Namespace, cmdline: str):
    start_time = time.time()
    result = repackage(
        args.input,
        args.prefix,
        subprefix=args.subprefix,
        size=args.size,
        warc_download_prefix=args.warc_download_prefix,
        wb=args.wb,
        processes=args.processes,
        parallel_readers=args.parallel_readers,
        metadata_records=args.add_metadata_record,
        sort=not args.no_sort,
        log_every_n=args.log_every_n,
        creator=args.creator,
        operator=args.operator,
        is_part_of=args.is_part_of,
        description=args.description or 'warc extraction generated with: ' + cmdline,
        input_format=args.input_format,
    )

    LOGGER.info('wrote %d of %d captures to WARC files (%d failed) in %.1f seconds',
                result.written, result.requested, result.failed, time.time() - start_time)
    if result.failed:
        raise IncompleteError(
            f'{result.failed} of {result.requested} captures could not be fetched (see the log), '
            f'{result.written} were written'
        )
