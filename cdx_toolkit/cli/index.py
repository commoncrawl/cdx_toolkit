from argparse import Namespace, RawDescriptionHelpFormatter
import logging

from cdx_toolkit.indexes import IndexQuery, UsageError, open_index
from cdx_toolkit.indexes.resolve import INDEX_FORMATS
from cdx_toolkit.record_io import (
    FORMAT_CHOICES, canonical_format, format_from_path, import_pyarrow, open_text, write_records,
)

from .common import add_verbose_arg, parse_fields, positive_int


LOGGER = logging.getLogger(__name__)

DESCRIPTION = '''\
Select captures from an index and write them as JSONL, CDXJ, CSV, TSV or Parquet.

INDEX is one of:
  cc, ia                     Common Crawl or Internet Archive CDX API
  http(s)://...              URL of a CDX API server
  cc-url-index-s3            Common Crawl's columnar URL index on S3, queried with DuckDB
  cc-url-index-hf            Common Crawl's columnar URL index on Hugging Face, queried with DuckDB
  PATH                       other columnar (parquet) index: *.parquet files, or a hive-partitioned
                             directory with --index-format duckdb
  athena://...               the columnar URL index queried with AWS Athena, e.g.
                             athena://athena.us-east-1.amazonaws.com/ccindex?S3OutputLocation=s3://bucket/path/
  PATH                       CDX file(s), local or remote, may be a glob, e.g.
                             's3://commoncrawl/cc-index/collections/CC-MAIN-2024-30/indexes/cdx-*.gz'
'''

EXAMPLES = '''\
examples:
  cdxt index cc --url 'commoncrawl.org/*' --limit 10
  cdxt index cc --domain commoncrawl.org --crawl CC-MAIN-2024-33 -o captures.csv
  cdxt index ia --url 'commoncrawl.org/*' --from 2008 --to 200906302359 -o captures.jsonl
  cdxt index cc --url 'commoncrawl.org/*' --count
  cdxt index cc-url-index-s3 --domain commoncrawl.org --crawl CC-MAIN-2024-33 -o captures.jsonl
  cdxt index cc-url-index-s3 --sql homepages.sql --yes -o captures.csv
  cdxt index 'indexes/cdx-*.gz' --url-file urls.txt -o filtered/  # one output per CDX file
  cdxt index cc --url 'commoncrawl.org/*' --limit 10 | cdxt repackage --prefix COMMONCRAWL
'''


def add_parser(subparsers):
    parser = subparsers.add_parser(
        'index',
        help='select captures from an index',
        description=DESCRIPTION,
        epilog=EXAMPLES,
        formatter_class=RawDescriptionHelpFormatter,
    )
    add_verbose_arg(parser, subcommand=True)
    parser.add_argument('index', metavar='INDEX', help='index name or location (see above)')
    parser.add_argument(
        '--index-format',
        choices=INDEX_FORMATS,
        default='auto',
        help='type of the index (default: detected from INDEX)',
    )

    selection = parser.add_argument_group(
        'capture selection',
        ('--url, --hostname, --domain and --url-file can be repeated and combined; a capture matches if it matches '
         'any of them.'),
    )
    selection.add_argument(
        '--url', action='append', dest='urls', default=[], metavar='PATTERN',
        help="URL pattern, e.g. 'example.com/*' (prefix), '*.example.com' (domain) or an exact URL",
    )
    selection.add_argument(
        '--hostname', action='append', dest='hostnames', default=[], metavar='HOST',
        help='all captures of this exact hostname, e.g. www.example.com',
    )
    selection.add_argument(
        '--domain', action='append', dest='domains', default=[], metavar='DOMAIN',
        help='all captures of this domain and its subdomains, e.g. example.com',
    )
    selection.add_argument(
        '--url-file', metavar='PATH',
        help=('file with URL patterns (like --url): one per line, or a column of a CSV, TSV, JSONL or Parquet file; '
              'CDX files only'),
    )
    selection.add_argument(
        '--url-column', metavar='NAME', help='column of the --url-file with the URL patterns (default: url)'
    )
    selection.add_argument(
        '--url-file-type', choices=('pattern', 'domain'),
        help=('how the --url-file entries match: pattern (default) like --url, e.g. example.com is only that URL; '
              'domain: example.com matches the domain and its subdomains, example.com/path all URLs starting with it'),
    )
    selection.add_argument(
        '--crawl',
        help=('Common Crawl crawl names (comma separated, e.g. CC-MAIN-2024-33 or CC-MAIN-2018) '
              'or an integer for the most recent N crawls'),
    )
    selection.add_argument('--from', dest='from_ts', metavar='TIMESTAMP', help='start time, e.g. 2008 or 20080328')
    selection.add_argument('--to', metavar='TIMESTAMP', help='end time, e.g. 2009 or 200906302359')
    selection.add_argument('--closest', metavar='TIMESTAMP', help='captures closest to this timestamp (CDX API only)')
    selection.add_argument(
        '--filter', action='append', dest='filters', default=[], metavar='[!~=]FIELD:EXPR',
        help='CDX API filter, e.g. =status:200 (repeatable; see README for the syntax)',
    )
    selection.add_argument(
        '--language', action='append', dest='languages', default=[], metavar='CODE',
        help=('only captures whose primary content language is CODE (ISO 639-3, e.g. eng or deu; repeatable); '
              'SQL indexes only'),
    )
    selection.add_argument('--url-fgrep', metavar='TEXT', help='only captures whose URL contains TEXT')
    selection.add_argument('--url-fgrepv', metavar='TEXT', help='only captures whose URL does not contain TEXT')
    selection.add_argument(
        '--limit', type=int,
        help='maximum number of captures to read from the index (applied before --url-fgrep/--url-fgrepv)',
    )
    selection.add_argument(
        '--sql', metavar='PATH',
        help=("file with a SQL query for Athena or DuckDB ('-' = stdin), used instead of "
              '--hostname/--domain/--crawl/--from/--to/--language/--limit'),
    )

    output = parser.add_argument_group('output')
    output.add_argument(
        '-o', '--output', default='-', metavar='PATH',
        help=("output file, local or remote (e.g. s3://...), gzipped if it ends with .gz (default: '-' = stdout); "
              'for CDX files, a path without file extension is a directory with one output per CDX file'),
    )
    output.add_argument(
        '--output-format', choices=FORMAT_CHOICES,
        help=('output format (default: detected from the --output extension, jsonl for stdout); cdx is the same as '
              "cdxj: Common Crawl's index files have the .cdx extension but use the CDXJ format (crawl index JSON); "
              'parquet needs cdx_toolkit[parquet]; without --fields, CSV, TSV and Parquet output of the CDX API and '
              'CDX files is written once all captures are read (to include all fields)'),
    )
    output.add_argument(
        '--fields', type=parse_fields, metavar='FIELD,...',
        help='comma-separated fields to output, e.g. url,timestamp,status (default: all fields)',
    )
    output.add_argument(
        '--count', action='store_true',
        help=('only print the number of captures, at most --limit (CDX API: cheap estimate based on the number of '
              'index pages, overlapping selectors are counted more than once; CDX files and SQL: exact count)'),
    )
    output.add_argument(
        '--dry-run', action='store_true',
        help='only print what would be queried (e.g. the SQL query), without querying the index',
    )

    cdx_files = parser.add_argument_group('CDX files options')
    cdx_files.add_argument(
        '--processes', type=positive_int, default=1, metavar='N',
        help='number of processes, with one output per CDX file (default: 1)',
    )

    sql = parser.add_argument_group('SQL options (Athena, DuckDB)')
    sql.add_argument(
        '--yes', action='store_true',
        help=('do not ask for confirmation before scanning more than 10 crawls '
              '(Athena bills per TB scanned; restrict with --crawl)'),
    )

    cdx_api = parser.add_argument_group('CDX API options')
    cdx_api.add_argument('--cc-mirror', metavar='URL', help='use this Common Crawl index mirror')
    cdx_api.add_argument('--cc-sort', choices=('mixed', 'ascending'), help='order of the crawls (default: mixed)')

    parser.set_defaults(func=run)
    return parser


def read_sql(path: str) -> str:
    with open_text(path, 'r') as fh:
        return fh.read()


def build_query(args: Namespace) -> IndexQuery:
    return IndexQuery(
        urls=args.urls,
        hostnames=args.hostnames,
        domains=args.domains,
        url_file=args.url_file,
        url_column=args.url_column,
        url_file_type=args.url_file_type,
        crawl=args.crawl,
        from_ts=args.from_ts,
        to=args.to,
        closest=args.closest,
        filters=args.filters,
        url_fgrep=args.url_fgrep,
        url_fgrepv=args.url_fgrepv,
        limit=args.limit,
        languages=args.languages,
        sql=read_sql(args.sql) if args.sql else None,
    )


def get_output_format(args: Namespace) -> str:
    fmt = canonical_format(args.output_format)
    if not fmt:
        fmt = 'jsonl' if args.output == '-' else format_from_path(args.output)
        if fmt is None:
            raise UsageError(f'cannot detect the output format of {args.output}, please set --output-format')
    if fmt == 'parquet':
        if args.output == '-':
            raise UsageError('Parquet output cannot be written to stdout, please set --output')
        try:
            import_pyarrow()
        except RuntimeError as e:
            raise UsageError(str(e)) from None
    return fmt


def writes_per_file(args: Namespace, backend) -> bool:
    """CDX files with an output path without file extension: one output per CDX file."""
    from cdx_toolkit.indexes.backends.cdx_files import CdxFilesBackend

    return (
        isinstance(backend, CdxFilesBackend)
        and not (args.count or args.dry_run or args.output_format)
        and args.output != '-'
        and format_from_path(args.output) is None
    )


def run(args: Namespace, cmdline: str):
    backend = open_index(
        args.index, args.index_format, cc_mirror=args.cc_mirror, cc_sort=args.cc_sort, assume_yes=args.yes
    )
    query = build_query(args)

    if writes_per_file(args, backend):
        if args.fields:
            raise UsageError('--fields cannot be used when writing one output per CDX file (the lines are copied)')
        n_files, n = backend.write_per_file(query, args.output, processes=args.processes)
        LOGGER.info('wrote %d captures to %d files in %s', n, n_files, args.output)
        return
    if args.processes > 1:
        raise UsageError('--processes can only be used with CDX files and one output per file (-o DIRECTORY)')

    output_format = None if args.count or args.dry_run else get_output_format(args)

    if args.dry_run:
        with open_text(args.output, 'w') as fh:
            fh.write(backend.explain(query))
        return

    if args.count:
        count = backend.count(query)
        with open_text(args.output, 'w') as fh:
            fh.write(f'{count}\n')
        return

    records = backend.records(query)
    n = write_records(records, args.output, output_format, args.fields, fixed_schema=backend.fixed_schema)
    LOGGER.info('wrote %d captures to %s', n, args.output)
