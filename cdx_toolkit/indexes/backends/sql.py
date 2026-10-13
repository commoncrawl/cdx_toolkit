'''
The common logic of the backends for the columnar URL index (parquet), queried with SQL (see duckdb.py, athena.py).
'''
import datetime
import logging
import re
import sys
from typing import Iterator, List, Mapping, Optional, Tuple

from ..base import IndexBackend, UsageError
from ..query import IndexQuery, field_flag
from .sql_builder import build_sql, query_conditions, resolve_crawl_names, url_surtkeys


LOGGER = logging.getLogger(__name__)

# columns of the columnar index selected by the guided (--hostname/--domain) queries,
# mapped to the field names of the CDX API so records look the same for every backend
COLUMNS = {
    'url_surtkey': 'urlkey',
    'fetch_time': 'timestamp',
    'url': 'url',
    'content_mime_type': 'mime',
    'content_mime_detected': 'mime-detected',
    'fetch_status': 'status',
    'content_digest': 'digest',
    'warc_record_length': 'length',
    'warc_record_offset': 'offset',
    'warc_filename': 'filename',
    'content_languages': 'languages',
    'content_charset': 'encoding',
    'fetch_redirect': 'redirect',
    'crawl': 'crawl',
}

# A query restricted to at most this many crawls is considered cheap enough to
# run without a cost-confirmation prompt.
LARGE_CRAWL_SET_THRESHOLD = 10

GUIDED_OPTIONS = ('urls', 'hostnames', 'domains', 'crawl', 'from_ts', 'to', 'languages', 'limit')


def confirm_cost(n_crawls: Optional[int], engine: str, confirmed: bool) -> None:
    """Prompt before running a potentially expensive index scan.

    `n_crawls` is the number of crawls the scan is restricted to, None if it is not restricted (or it cannot be
    verified, e.g. for raw SQL). A scan bounded to <= LARGE_CRAWL_SET_THRESHOLD crawls runs without a prompt.
    Otherwise we confirm interactively, abort in non-interactive sessions, unless `confirmed` (--yes)."""
    if confirmed:
        return

    if n_crawls is not None and n_crawls <= LARGE_CRAWL_SET_THRESHOLD:
        return

    if n_crawls is None:
        reason = (f'This {engine} query is not restricted to specific crawls (or uses custom SQL '
                  'whose crawl partition pruning could not be verified) and may scan ALL crawls.')
    else:
        reason = (f'This {engine} query is restricted to {n_crawls} crawls (more than '
                  f'{LARGE_CRAWL_SET_THRESHOLD}) and may scan a large amount of data.')

    if not sys.stdin.isatty():
        raise SystemExit(
            reason + ' Refusing to run a potentially expensive scan in non-interactive mode. '
            f'Restrict with --crawl (<={LARGE_CRAWL_SET_THRESHOLD} crawls), or pass --yes.'
        )

    LOGGER.warning(reason)
    # the prompt goes to stderr, stdout may be the output of the captures
    sys.stderr.write('Index SQL scans can be expensive (Athena bills per TB scanned). Proceed? [y/N] ')
    sys.stderr.flush()
    answer = input()
    if answer.strip().lower() not in ('y', 'yes'):
        raise SystemExit('Aborted by user.')


def normalize_row(row: Mapping) -> dict:
    """Columnar index row -> record with CDX field names; timestamps as YYYYMMDDhhmmss strings.

    NULL values stay None, so that every record has all columns (see IndexBackend.fixed_schema)."""
    record = {}
    for column, value in row.items():
        if value is None:
            pass
        elif isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                value = value.astimezone(datetime.timezone.utc)
            value = value.strftime('%Y%m%d%H%M%S')
        elif column == 'fetch_time':
            value = re.sub(r'\D', '', str(value))[:14]  # Athena: '2024-07-13 10:22:33.000'
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            value = str(value)  # the CDX API returns strings
        record[COLUMNS.get(column, column)] = value
    return record


class SqlBackend(IndexBackend):
    """Common logic of the SQL backends: guided query building, cost guard, row normalization."""

    engine = ''
    fixed_schema = True
    supported_options = frozenset({
        'urls', 'hostnames', 'domains', 'crawl', 'from_ts', 'to', 'languages', 'limit', 'sql', 'url_fgrep',
        'url_fgrepv',
    })

    def __init__(self, location: str, assume_yes: bool = False):
        self.location = location
        self.assume_yes = assume_yes

    url_file_hint = ''

    def validate(self, query: IndexQuery) -> None:
        if query.url_file or query.url_column or query.url_file_type:
            raise UsageError(
                f'--url-file cannot be used with the {self.name} index; instead, use a custom --sql query that joins '
                f'the index with a table of the target URLs, e.g.{self.url_file_hint}'
            )
        super().validate(query)
        if query.sql:
            combined = [field_flag(option) for option in GUIDED_OPTIONS if option in query.options_used()]
            if combined:
                raise UsageError(f'{", ".join(combined)} cannot be combined with --sql')
        elif not (query.urls or query.hostnames or query.domains):
            raise UsageError(f'the {self.name} index needs --url, --hostname, --domain or --sql')
        else:
            url_surtkeys(query)  # check --url
            query_conditions(query)  # check --from, --to and --language

    def from_clause(self, crawls: Optional[List[str]]) -> Tuple[str, bool]:
        """FROM expression for the guided query, and whether it already prunes the crawls."""
        raise NotImplementedError  # pragma: no cover

    def iter_sql_rows(self, sql: str) -> Iterator[dict]:
        raise NotImplementedError  # pragma: no cover

    def prepare(self, query: IndexQuery, order_by: bool = True) -> Tuple[str, Optional[int]]:
        """The SQL to run and the number of crawls it is restricted to (None = unknown)."""
        if query.sql:
            return query.sql.strip().rstrip(';'), None
        crawls = resolve_crawl_names(query.crawl) if query.crawl else None
        from_clause, prunes_crawls = self.from_clause(crawls)
        surtkey_prefixes, surtkeys = url_surtkeys(query)
        sql = build_sql(
            from_clause,
            query.hostnames,
            crawls=None if prunes_crawls else crawls,
            limit=query.limit or 0,
            url_host_registered_domains=query.domains,
            order_by=order_by,
            columns=list(COLUMNS),
            conditions=query_conditions(query),
            url_surtkey_prefixes=surtkey_prefixes,
            url_surtkeys=surtkeys,
        )
        return sql, len(crawls) if crawls else None

    def iter_records(self, query: IndexQuery) -> Iterator[Mapping]:
        sql, n_crawls = self.prepare(query)
        confirm_cost(n_crawls, self.engine, self.assume_yes)
        for row in self.iter_sql_rows(sql):
            yield normalize_row(row)

    def count(self, query: IndexQuery) -> int:
        sql, n_crawls = self.prepare(query, order_by=False)
        confirm_cost(n_crawls, self.engine, self.assume_yes)
        row = next(self.iter_sql_rows(f'SELECT count(*) AS n FROM ({sql}) AS q'))
        return int(row['n'])

    def explain(self, query: IndexQuery) -> str:
        sql, n_crawls = self.prepare(query)
        scope = f'{n_crawls} crawl(s)' if n_crawls else 'all crawls (or not verifiable)'
        return f'-- {self.name} query on {self.location}, restricted to {scope}\n{sql.strip()}\n'
