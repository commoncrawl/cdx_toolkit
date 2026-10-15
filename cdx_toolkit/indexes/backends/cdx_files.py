'''
Index backend for CDX files (e.g. Common Crawl's cc-index/collections/*/indexes/cdx-*.gz), local or remote.

Each line is `<SURT key> <timestamp> <JSON>` (CDXJ). Captures are selected by matching the SURT key
against SURT prefixes (with url-is-in) or exact SURT keys, built from --url, --hostname, --domain
and --url-file.
'''
import logging
import os
from dataclasses import dataclass
from itertools import islice
from typing import Iterator, List, Mapping, NamedTuple, Optional, Tuple

from cdx_toolkit.aws import process_pool
from cdx_toolkit.record_io import (
    check_glob, format_from_path, is_empty_dir, is_glob, open_text, parse_cdxj_line, read_records,
)
from cdx_toolkit.timeutils import pad_timestamp, pad_timestamp_up

from ..base import IndexBackend, QueryPlan, UsageError
from ..patterns import (
    domain_entry_to_surts, domain_to_surts, hostname_to_surts, normalize_host, url_hostname, url_pattern_to_surts,
)
from ..query import IndexQuery


LOGGER = logging.getLogger(__name__)

GLOB_CHARS = ('*', '?', '[')

# file formats of --url-file that have columns (others: one URL pattern per line)
TABLE_FORMATS = ('csv', 'tsv', 'jsonl', 'parquet')


URL_FILE_TYPES = ('pattern', 'domain')


def load_url_patterns(path: str, column: Optional[str] = None) -> List[str]:
    """URL patterns from a file: one per line (e.g. .txt), or a column of a CSV, TSV, JSONL or Parquet file."""
    fmt = format_from_path(path)
    if fmt in TABLE_FORMATS:
        column = column or 'url'
        patterns = []
        for i, record in enumerate(read_records(path, fmt)):
            if column not in record:
                if i == 0:
                    raise UsageError(f'--url-file {path} has no column {column!r} (use --url-column)')
                continue
            if record[column]:
                patterns.append(str(record[column]))
        return patterns
    if column:
        raise UsageError(f'--url-column needs a CSV, TSV, JSONL or Parquet --url-file, not {path}')
    with open_text(path, 'r') as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith('#')]


@dataclass(frozen=True)
class CaptureFilter:
    """Which lines of a CDX file are selected (picklable, for worker processes)."""

    prefixes: Tuple[str, ...] = ()
    exact: Tuple[str, ...] = ()
    # --hostname: the SURT prefixes select the candidates (with and without www.), the URL's host must be one of
    # the hostnames
    host_prefixes: Tuple[str, ...] = ()
    hostnames: Tuple[str, ...] = ()
    select_all: bool = False
    from_ts: Optional[str] = None
    to: Optional[str] = None
    url_fgrep: Optional[str] = None
    url_fgrepv: Optional[str] = None

    @classmethod
    def from_query(cls, query: IndexQuery, url_filters: bool = False) -> 'CaptureFilter':
        """The selection of the query; --url-fgrep/--url-fgrepv only with `url_filters` (one output per file:
        without --limit, they can be applied while reading; otherwise they are applied after the limit)."""
        prefixes, exact, host_prefixes = [], [], []
        patterns = list(query.urls)
        if query.url_file:
            entries = load_url_patterns(query.url_file, query.url_column)
            LOGGER.info('loaded %d entries from %s', len(entries), query.url_file)
            if (query.url_file_type or 'pattern') == 'domain':
                for entry in entries:
                    prefixes += domain_entry_to_surts(entry)
            else:
                patterns += entries
        for pattern in patterns:
            p, e = url_pattern_to_surts(pattern)
            prefixes += p
            exact += e
        for hostname in query.hostnames:
            host_prefixes += hostname_to_surts(hostname)
        for domain in query.domains:
            prefixes += domain_to_surts(domain)
        return cls(
            prefixes=tuple(prefixes),
            exact=tuple(exact),
            host_prefixes=tuple(host_prefixes),
            hostnames=tuple(normalize_host(hostname) for hostname in query.hostnames),
            select_all=not (query.urls or query.url_file or query.hostnames or query.domains),
            from_ts=pad_timestamp(query.from_ts) if query.from_ts else None,
            to=pad_timestamp_up(query.to) if query.to else None,
            url_fgrep=query.url_fgrep if url_filters else None,
            url_fgrepv=query.url_fgrepv if url_filters else None,
        )

    def matcher(self) -> 'LineMatcher':
        return LineMatcher(self)


class LineMatcher:
    def __init__(self, capture_filter: CaptureFilter):
        from url_is_in import SURTMatcher

        self.filter = capture_filter
        self.exact = set(capture_filter.exact)
        self.prefixes = None
        if capture_filter.prefixes:
            self.prefixes = SURTMatcher(list(capture_filter.prefixes), match_subdomains=False)
        self.host_prefixes = None
        if capture_filter.host_prefixes:
            self.host_prefixes = SURTMatcher(list(capture_filter.host_prefixes), match_subdomains=False)
        self.hostnames = set(capture_filter.hostnames)

    def selected(self, key: str, line: str) -> bool:
        """Selected by --url, --hostname, --domain or --url-file."""
        if self.filter.select_all or key in self.exact or (self.prefixes and self.prefixes.is_in(key)):
            return True
        if self.host_prefixes and self.host_prefixes.is_in(key):
            return url_hostname(parse_cdxj_line(line).get('url', '')) in self.hostnames
        return False

    def matches(self, line: str) -> bool:
        key, _, rest = line.partition(' ')
        f = self.filter
        if not self.selected(key, line):
            return False
        if f.from_ts or f.to:
            timestamp = rest[:rest.find(' ')]
            if (f.from_ts and timestamp < f.from_ts) or (f.to and timestamp > f.to):
                return False
        if f.url_fgrep or f.url_fgrepv:
            url = parse_cdxj_line(line).get('url', '')
            if (f.url_fgrep and f.url_fgrep not in url) or (f.url_fgrepv and f.url_fgrepv in url):
                return False
        return True


def iter_matching_lines(path: str, matcher: LineMatcher) -> Iterator[str]:
    n_lines = n_matches = 0
    with open_text(path, 'r') as fh:
        for line in fh:
            n_lines += 1
            if line.strip() and matcher.matches(line):
                n_matches += 1
                yield line
    LOGGER.info('%s: %d of %d lines selected', path, n_matches, n_lines)


def _filter_file(task: Tuple[str, str, CaptureFilter]) -> Tuple[str, int]:
    """Write the selected lines of one CDX file to its own output (worker process); no file without matches."""
    input_path, output_path, capture_filter = task
    matcher = capture_filter.matcher()
    n = 0
    fh = ctx = None
    try:
        for line in iter_matching_lines(input_path, matcher):
            if fh is None:
                if '://' not in output_path:
                    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
                ctx = open_text(output_path, 'w')
                fh = ctx.__enter__()
            fh.write(line if line.endswith('\n') else line + '\n')
            n += 1
    finally:
        if ctx is not None:
            ctx.__exit__(None, None, None)
    return output_path, n


class CdxFilesBackend(IndexBackend):
    """CDX files (CDXJ, optionally gzipped), local or remote (fsspec), a single file or a glob."""

    name = 'CDX files'
    supported_options = frozenset({
        'urls', 'hostnames', 'domains', 'url_file', 'url_column', 'url_file_type', 'from_ts', 'to',
        'url_fgrep', 'url_fgrepv', 'limit',
    })

    def __init__(self, location: str):
        self.location = location

    def validate(self, query: IndexQuery) -> None:
        super().validate(query)
        try:
            check_glob(self.location)
        except ValueError as e:
            raise UsageError(str(e)) from None
        if (query.url_column or query.url_file_type) and not query.url_file:
            raise UsageError('--url-column and --url-file-type need --url-file')
        if query.url_file_type and query.url_file_type not in URL_FILE_TYPES:
            choices = ', '.join(URL_FILE_TYPES)
            raise UsageError(f'unknown --url-file-type {query.url_file_type!r} (choose from {choices})')
        if not (query.urls or query.url_file or query.hostnames or query.domains or query.from_ts or query.to):
            raise UsageError(
                f'select captures from the {self.name} with --url, --hostname, --domain, --url-file, --from or --to'
            )
        for timestamp in (query.from_ts, query.to):
            if timestamp and not (timestamp.isdigit() and 4 <= len(timestamp) <= 14):
                raise UsageError(f'invalid timestamp {timestamp!r}, use e.g. 2008, 20080328 or 200906302359')
        for pattern in query.urls:
            url_pattern_to_surts(pattern)

    def input_locations(self, query: IndexQuery) -> List[str]:
        return [self.location] + ([query.url_file] if query.url_file else [])

    def files(self) -> List['CdxFile']:
        """The CDX files of the location (a file or a glob)."""
        import fsspec

        if not is_glob(self.location):
            if '://' in self.location:
                return [CdxFile(self.location, self.location.split('?', 1)[0].rstrip('/').rsplit('/', 1)[-1])]
            return [CdxFile(self.location, os.path.basename(self.location))]  # also with backslashes on Windows
        fs, fs_path = fsspec.url_to_fs(self.location)
        first_glob = min(fs_path.index(c) for c in GLOB_CHARS if c in fs_path)
        base = fs_path[:fs_path.rfind('/', 0, first_glob)]
        paths = sorted(fs.glob(fs_path))
        if not paths:
            raise FileNotFoundError(f'no files found for {self.location}')
        return [CdxFile(fs.unstrip_protocol(p), p[len(base):].lstrip('/')) for p in paths]

    def plan(self, query: IndexQuery) -> 'CdxFilesPlan':
        return CdxFilesPlan(self.location, self.files(), CaptureFilter.from_query(query))

    def select(self, query: IndexQuery) -> Iterator[Mapping]:
        plan = self.plan(query)
        matcher = plan.capture_filter.matcher()
        for cdx_file in plan.files:
            for line in iter_matching_lines(cdx_file.path, matcher):
                yield parse_cdxj_line(line)

    def select_count(self, query: IndexQuery) -> int:
        """Exact number of selected captures, at most --limit (reads all files, or until the limit)."""
        return sum(1 for _ in islice(self.select(query), query.limit))

    def write_per_file(self, query: IndexQuery, output_base: str, processes: int = 1) -> Tuple[int, int]:
        """Write the selected lines of each CDX file, unchanged, to <output_base>/<relative path of the file>.

        Returns the number of output files and captures; no file is written without matches."""
        self.validate(query)
        if query.limit:
            raise UsageError('--limit cannot be used when writing one output per CDX file')
        if not is_empty_dir(output_base):
            raise UsageError(
                f'the output directory {output_base} is not empty; use a new directory, so that no output of an '
                'earlier run is mixed with this one'
            )
        capture_filter = CaptureFilter.from_query(query, url_filters=True)
        join = (lambda rel: output_base.rstrip('/') + '/' + rel) if '://' in output_base else \
            (lambda rel: os.path.join(output_base, *rel.split('/')))
        tasks = [(f.path, join(f.relative_path), capture_filter) for f in self.files()]

        if processes > 1:
            with process_pool(processes) as ex:
                results = list(ex.map(_filter_file, tasks))
        else:
            results = [_filter_file(task) for task in tasks]

        written = [(path, n) for path, n in results if n]
        LOGGER.info('wrote %d captures to %d of %d files', sum(n for _, n in written), len(written), len(tasks))
        return len(written), sum(n for _, n in written)


class CdxFile(NamedTuple):
    path: str
    relative_path: str  # to the base directory of the glob (the path of the output with one output per file)


@dataclass
class CdxFilesPlan(QueryPlan):
    """The CDX files and which of their lines are selected."""

    location: str
    files: List[CdxFile]
    capture_filter: CaptureFilter

    def describe(self) -> str:
        f = self.capture_filter
        lines = [f'{len(self.files)} CDX file(s): {self.location}']
        lines += [f'  {cdx_file.path}' for cdx_file in self.files[:20]] + (['  ...'] if len(self.files) > 20 else [])
        lines.append(f'{len(f.prefixes)} SURT prefix(es), {len(f.exact)} exact URL(s)')
        lines += [f'  prefix {p}' for p in f.prefixes[:20]]
        lines += [f'  exact  {e}' for e in f.exact[:20]]
        if f.from_ts or f.to:
            lines.append(f'timestamps: {f.from_ts or "-"} to {f.to or "-"}')
        return '\n'.join(lines) + '\n'
