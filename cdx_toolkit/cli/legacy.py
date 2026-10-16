'''
The command lines of cdx_toolkit 0.9.39 and earlier, e.g. `cdxt --cc --limit 10 iter 'example.com/*'`: options before
the command, and the commands `iter`, `size` and `warc`. They are deprecated: they are parsed with the old grammar and
run with the current code (CDX API backend, repackage()), printing a suggested new command line on stderr.

Differences to 0.9.39: `warc` writes <prefix>[-<subprefix>]-001.warc.gz (was -000000.extracted.warc.gz), copies the
WARC records byte-for-byte, writes no file without captures, and exits with status 1 if a capture could not be
fetched; `iter --all-fields --csv` is a usage error (it failed with a traceback).
'''
from argparse import ArgumentParser, Namespace
import csv
import json
import logging
import re
import shlex
import sys
from typing import Iterable, List, Mapping

from cdx_toolkit.indexes import IndexQuery, UsageError, open_index
from cdx_toolkit.indexes.base import filter_by_url
from cdx_toolkit.utils import get_version

from .common import IncompleteError


LOGGER = logging.getLogger(__name__)

LEGACY_COMMANDS = ('iter', 'size', 'warc')

DOCS_URL = 'https://github.com/commoncrawl/cdx_toolkit#command-line-tools'

# options of the old command line (before the command) that take a value
GLOBAL_VALUE_OPTIONS = {
    '--crawl', '--source', '--wb', '--limit', '--cc-mirror', '--cc-sort', '--from', '--to', '--filter', '--closest',
}

DEFAULT_FIELDS = 'url,status,timestamp'


def is_new_top_level_option(token: str) -> bool:
    """The options of the new command line before the command: -h, -V and -v."""
    return token in ('-h', '--help', '-V', '--version', '--verbose') or re.fullmatch(r'-v+', token) is not None


def is_legacy_command_line(args: List[str]) -> bool:
    """A command line of cdx_toolkit 0.9.39: the command is iter, size or warc, or options other than -h, -V and -v
    come before the command (the new command line has all other options after the command). The values of the old
    options are skipped, so `cdxt --source iter ...` is not taken for the command `iter`."""
    before_command = []
    command = None
    tokens = iter(args)
    for token in tokens:
        if token == '--':
            command = next(tokens, None)
            break
        if token.startswith('-') and token != '-':
            before_command.append(token)
            if '=' not in token and token in GLOBAL_VALUE_OPTIONS:
                next(tokens, None)
            continue
        command = token
        break
    return command in LEGACY_COMMANDS or not all(is_new_top_level_option(token) for token in before_command)


def build_legacy_parser() -> ArgumentParser:
    """The command line of cdx_toolkit 0.9.39."""
    parser = ArgumentParser(prog='cdxt', description='cdx_toolkit iterator command line tool (deprecated syntax)')

    parser.add_argument('--version', '-V', action='version', version=get_version())
    parser.add_argument('--verbose', '-v', action='count', help='set logging level to INFO (-v) or DEBUG (-vv)')
    parser.add_argument('--cc', action='store_const', const='cc', help='direct the query to the Common Crawl CDX/WARCs')
    parser.add_argument(
        '--crawl',
        action='store',
        help='crawl names (comma separated) or an integer for the most recent N crawls. Implies --cc',
    )
    parser.add_argument(
        '--ia', action='store_const', const='ia', help='direct the query to the Internet Archive CDX/wayback'
    )
    parser.add_argument('--source', action='store', help='direct the query to this CDX server')
    parser.add_argument('--wb', action='store', help='direct replays for content to this wayback')
    parser.add_argument('--limit', type=int, action='store')
    parser.add_argument('--cc-mirror', action='store', help='use this Common Crawl index mirror')
    parser.add_argument('--cc-sort', action='store', help='default mixed, alternatively: ascending')
    parser.add_argument('--from', action='store')
    parser.add_argument('--to', action='store')
    parser.add_argument('--filter', action='append', help='see CDX API documentation for usage')
    parser.add_argument(
        '--get', action='store_true', help='use a single get instead of a paged iteration. default limit=1000'
    )
    parser.add_argument(
        '--closest', action='store', help='get the closest capture to this timestamp. use with --get'
    )

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    iterate = subparsers.add_parser('iter', help='iterate printing captures')
    iterate.add_argument('--all-fields', action='store_true')
    iterate.add_argument(
        '--fields', action='store', default=DEFAULT_FIELDS, help='try --all-fields if you need the list'
    )
    iterate.add_argument('--jsonl', action='store_true')
    iterate.add_argument('--csv', action='store_true')
    iterate.add_argument('url')
    iterate.set_defaults(func=run_iter)

    warc = subparsers.add_parser('warc', help='iterate over capture content, creating a warc')
    warc.add_argument('--prefix', default='TEST', help='prefix for the warc filename')
    warc.add_argument('--subprefix', type=str, default=None, help='subprefix for the warc filename, default None')
    warc.add_argument('--size', type=int, default=1000000000, help='target for the warc filesize in bytes')
    warc.add_argument('--creator', action='store', help='creator of the warc: person, organization, service')
    warc.add_argument('--operator', action='store', help='a person, if the creator is an organization')
    warc.add_argument('--url-fgrep', action='store', help='this pattern must be present to warc an url')
    warc.add_argument(
        '--url-fgrepv', action='store', help='this pattern must not be present to warc an url, e.g. /robots.txt'
    )
    warc.add_argument(
        '--warc-download-prefix', action='store', help='prefix for downloading content, automatically set for CC'
    )
    warc.add_argument('url')
    warc.set_defaults(func=run_warc)

    size = subparsers.add_parser('size', help='imprecise count of how many results are available')
    size.add_argument('--details', action='store_true', help='show details of each subindex')
    size.add_argument('url')
    size.set_defaults(func=run_size)

    return parser


def legacy_source(ns: Namespace) -> str:
    """The CDX API of the old options: --crawl implies cc, otherwise --cc, --ia, then --source."""
    source = 'cc' if ns.crawl else ns.cc or ns.ia or ns.source
    if source is None:
        raise UsageError('must specify --cc, --ia, or a --source')
    if source not in ('cc', 'ia') and not source.startswith(('http://', 'https://')):
        raise UsageError(f'--source {source}: use the http(s) URL of a CDX API server')
    return source


def request_kwargs(ns: Namespace) -> dict:
    """The parameters of the CDX API requests (CDXFetcher.iter/get/get_size_estimate), as in 0.9.39."""
    kwargs = {}
    if ns.limit:
        kwargs['limit'] = ns.limit
    if getattr(ns, 'from'):
        kwargs['from_ts'] = getattr(ns, 'from')
    if ns.to:
        kwargs['to'] = ns.to
    if ns.closest:
        kwargs['closest'] = ns.closest
    if ns.filter:
        kwargs['filter'] = ns.filter
    return kwargs


def cc_options(ns: Namespace, source: str) -> dict:
    """--cc-mirror and --cc-sort; the old command line ignored them for other sources."""
    if source != 'cc':
        return {}
    return {'cc_mirror': ns.cc_mirror, 'cc_sort': ns.cc_sort}


def fetcher(ns: Namespace, source: str):
    """A CDXFetcher for the requests that the index backend does not make as 0.9.39 did (--get, --closest, size)."""
    import cdx_toolkit
    from cdx_toolkit.commoncrawl import normalize_crawl

    kwargs = {'source': source}
    for name, value in cc_options(ns, source).items():
        if value:
            kwargs[name] = value
    if ns.crawl:
        kwargs['crawl'] = normalize_crawl([ns.crawl])
    return cdx_toolkit.CDXFetcher(**kwargs)


def legacy_captures(ns: Namespace, source: str, use_get: bool) -> Iterable[Mapping]:
    """The captures of the old command line: a single (non-paged) request with --get, the paged iteration with the
    closest= parameter for --closest without --get, otherwise the CDX API backend (the same requests)."""
    if use_get:
        return fetcher(ns, source).get(ns.url, **request_kwargs(ns))
    if ns.closest:
        return fetcher(ns, source).iter(ns.url, **request_kwargs(ns))
    backend = open_index(source, 'cdx-api', **cc_options(ns, source))
    query = IndexQuery(
        urls=[ns.url],
        crawl=ns.crawl,
        from_ts=getattr(ns, 'from'),
        to=ns.to,
        filters=list(ns.filter or []),
        limit=ns.limit or None,
    )
    return backend.records(query)


def iter_fields(ns: Namespace) -> List[str]:
    return sorted(set(ns.fields.split(',')))


def run_iter(ns: Namespace, cmdline: str):
    if ns.all_fields and ns.csv:
        # 0.9.39 wrote the header of --fields, then failed with a traceback at the first capture
        raise UsageError('--all-fields cannot be used with --csv, select the columns with --fields')
    source = legacy_source(ns)
    fields = iter_fields(ns)

    writer = None
    if ns.csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=fields)
        writer.writeheader()

    for capture in legacy_captures(ns, source, use_get=ns.get):
        if ns.all_fields:
            selected = dict(capture)
        else:
            selected = {k: capture[k] for k in fields if k in capture}
        if ns.jsonl:
            print(json.dumps(selected, sort_keys=True))
        elif writer:
            writer.writerow(selected)
        else:
            print(', '.join(f'{k} {selected[k]}' for k in sorted(selected)))


def run_size(ns: Namespace, cmdline: str):
    kwargs = request_kwargs(ns)
    if ns.details:
        kwargs['details'] = True  # get_size_estimate() prints the estimate of each index
    print(fetcher(ns, legacy_source(ns)).get_size_estimate(ns.url, **kwargs))


def warc_content_source(ns: Namespace, source: str) -> dict:
    """Where `warc` gets the content of the captures from (as CDXFetcher did): {'wb': ...} or
    {'warc_download_prefix': ...}."""
    if ns.wb and ns.warc_download_prefix:
        raise UsageError('cannot specify both --wb and --warc-download-prefix')
    if ns.wb:
        if source == 'cc':
            raise UsageError('cannot specify --wb for Common Crawl (--cc, --crawl)')
        return {'wb': ns.wb}
    if ns.warc_download_prefix:
        return {'warc_download_prefix': ns.warc_download_prefix}
    if source == 'cc':
        return {'warc_download_prefix': 'https://data.commoncrawl.org'}
    if source == 'ia':
        return {'wb': 'ia'}
    raise UsageError('warc with --source needs --wb (a wayback machine) or --warc-download-prefix')


def run_warc(ns: Namespace, cmdline: str):
    from cdx_toolkit.repackage import repackage

    source = legacy_source(ns)
    content = warc_content_source(ns, source)
    # 0.9.39 iterated (paged) also with --get; the limit applies before --url-fgrep/--url-fgrepv
    captures = filter_by_url(legacy_captures(ns, source, use_get=False), ns.url_fgrep, ns.url_fgrepv)
    result = repackage(
        captures,
        ns.prefix,
        subprefix=ns.subprefix,
        size=ns.size,
        creator=ns.creator,
        operator=ns.operator,
        description='warc extraction generated with: ' + cmdline,
        sort=False,  # stream the captures in index order, as 0.9.39 did
        **content,
    )
    LOGGER.info('wrote %d of %d captures to WARC files (%d failed)', result.written, result.requested, result.failed)
    if result.failed:
        raise IncompleteError(
            f'{result.failed} of {result.requested} captures could not be fetched (see the log), '
            f'{result.written} were written'
        )


def suggested_replacement(ns: Namespace) -> str:
    """The new command line that does (about) the same; not exact for the text output of iter, --get, --closest
    without --get and size --details."""
    source = 'cc' if ns.crawl else ns.cc or ns.ia or ns.source or 'INDEX'
    index = ['cdxt', 'index', source, '--url', ns.url]
    if ns.crawl:
        index += ['--crawl', ns.crawl]
    if ns.limit:
        index += ['--limit', str(ns.limit)]
    elif ns.cmd == 'iter' and ns.get:
        index += ['--limit', '1000']  # the default limit of a single get
    for option, value in (('--from', getattr(ns, 'from')), ('--to', ns.to), ('--closest', ns.closest)):
        if value:
            index += [option, value]
    for expression in ns.filter or []:
        index += ['--filter', expression]
    if source == 'cc':
        for option, value in (('--cc-mirror', ns.cc_mirror), ('--cc-sort', ns.cc_sort)):
            if value:
                index += [option, value]

    if ns.cmd == 'iter':
        fields = [] if ns.all_fields else ['--fields', ','.join(iter_fields(ns))]
        if ns.jsonl:
            index += fields
        else:  # CSV instead of the old text output
            index += ['--output-format', 'csv'] + fields
        return render(index)
    if ns.cmd == 'size':
        return render(index + ['--count'])

    for option, value in (('--url-fgrep', ns.url_fgrep), ('--url-fgrepv', ns.url_fgrepv)):
        if value:
            index += [option, value]
    repackage = ['cdxt', 'repackage', '--prefix', ns.prefix]
    if ns.subprefix:
        repackage += ['--subprefix', ns.subprefix]
    if ns.size != 1000000000:
        repackage += ['--size', str(ns.size)]
    for option, value in (('--creator', ns.creator), ('--operator', ns.operator)):
        if value:
            repackage += [option, value]
    try:
        content = warc_content_source(ns, legacy_source(ns))
    except UsageError:
        content = {}
    if 'wb' in content:
        repackage += ['--wb', content['wb']]
    elif content.get('warc_download_prefix', 'https://data.commoncrawl.org') != 'https://data.commoncrawl.org':
        repackage += ['--warc-download-prefix', content['warc_download_prefix']]
    return render(index) + ' | ' + render(repackage + ['--no-sort'])


def render(tokens: List[str]) -> str:
    return ' '.join(shlex.quote(token) for token in tokens)


def deprecation_message(ns: Namespace) -> str:
    return (
        'cdxt: this command line uses the old syntax, which is deprecated and will be removed in cdx_toolkit 1.0; '
        f'suggested replacement: {suggested_replacement(ns)} (see {DOCS_URL})\n'
    )
