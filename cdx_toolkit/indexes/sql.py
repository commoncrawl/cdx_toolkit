'''
Index backends for the columnar URL index (parquet), queried with SQL via AWS Athena or DuckDB.
'''
import datetime
import logging
import re
import sys
from typing import Iterator, List, Mapping, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from cdx_toolkit.repackage.sources.base import CostEstimate
from cdx_toolkit.timeutils import pad_timestamp, pad_timestamp_up
from cdx_toolkit.repackage.sources.sql_base import build_sql, resolve_crawl_names

from .base import IndexBackend, UsageError
from .patterns import url_pattern_to_surts
from .query import IndexQuery, field_flag


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


def confirm_cost(estimate: Optional[CostEstimate], confirmed: bool) -> None:
    """Prompt before running a potentially expensive index scan.

    A scan bounded to <= LARGE_CRAWL_SET_THRESHOLD crawls runs without a prompt.
    Otherwise (no crawl filter, a large crawl set, or unverifiable raw SQL) we confirm
    interactively, abort in non-interactive sessions, unless `confirmed` (--yes)."""
    if confirmed or estimate is None:
        return

    n_crawls = estimate.n_crawls
    if n_crawls is not None and n_crawls <= LARGE_CRAWL_SET_THRESHOLD:
        return

    engine = estimate.engine
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


def sql_timestamp(timestamp: str, up: bool = False) -> str:
    """CDX timestamp (e.g. 2008, 200906302359) -> SQL TIMESTAMP literal of its first (or last) second."""
    if not re.fullmatch(r'\d{4,14}', timestamp):
        raise UsageError(f'invalid timestamp {timestamp!r}, use e.g. 2008, 20080328 or 200906302359')
    padded = pad_timestamp_up(timestamp) if up else pad_timestamp(timestamp)
    try:
        moment = datetime.datetime.strptime(padded, '%Y%m%d%H%M%S')
    except ValueError:
        raise UsageError(f'invalid timestamp {timestamp!r}') from None
    return f"TIMESTAMP '{moment:%Y-%m-%d %H:%M:%S}'"


def check_language(code: str) -> str:
    """An ISO 639-3 language code (three letters, e.g. eng or deu), in lowercase."""
    language = code.strip().lower()
    if not re.fullmatch(r'[a-z]{3}', language):
        raise UsageError(f'invalid language {code!r}, use ISO 639-3 codes (three letters), e.g. eng or deu')
    return language


def url_surtkeys(query: IndexQuery) -> Tuple[List[str], List[str]]:
    """--url patterns -> (url_surtkey prefixes, exact url_surtkey values)."""
    prefixes, exact = [], []
    for pattern in query.urls:
        p, e = url_pattern_to_surts(pattern)
        prefixes += p
        exact += e
    return prefixes, exact


def query_conditions(query: IndexQuery) -> List[str]:
    """WHERE predicates for --from, --to and --language."""
    conditions = []
    if query.from_ts:
        conditions.append(f'fetch_time >= {sql_timestamp(query.from_ts)}')
    if query.to:
        conditions.append(f'fetch_time <= {sql_timestamp(query.to, up=True)}')
    if query.languages:
        # primary content language: the first of the comma-separated content_languages
        predicates = []
        for code in query.languages:
            language = check_language(code)
            predicates += [f"content_languages = '{language}'", f"content_languages LIKE '{language},%'"]
        conditions.append('(' + ' OR '.join(predicates) + ')')
    return conditions


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
        self._prepared = None

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
        confirm_cost(CostEstimate(n_crawls=n_crawls, engine=self.engine), self.assume_yes)
        for row in self.iter_sql_rows(sql):
            yield normalize_row(row)

    def count(self, query: IndexQuery) -> int:
        sql, n_crawls = self.prepare(query, order_by=False)
        confirm_cost(CostEstimate(n_crawls=n_crawls, engine=self.engine), self.assume_yes)
        row = next(self.iter_sql_rows(f'SELECT count(*) AS n FROM ({sql}) AS q'))
        return int(row['n'])

    def explain(self, query: IndexQuery) -> str:
        sql, n_crawls = self.prepare(query)
        scope = f'{n_crawls} crawl(s)' if n_crawls else 'all crawls (or not verifiable)'
        return f'-- {self.name} query on {self.location}, restricted to {scope}\n{sql.strip()}\n'


class DuckDbBackend(SqlBackend):
    """The columnar index queried with DuckDB: a hive-partitioned directory or parquet files (local, s3, hf)."""

    name = 'DuckDB'
    engine = 'duckdb'
    url_file_hint = (
        ':\n\n'
        '  SELECT idx.url, idx.warc_filename, idx.warc_record_offset, idx.warc_record_length\n'
        "  FROM read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-33/subset=warc/"
        "*.parquet') AS idx\n"
        "  JOIN read_csv('urls.csv') AS target ON idx.url = target.url"
    )

    def __init__(self, location: str, assume_yes: bool = False):
        if "'" in location:
            raise UsageError(f'invalid index location {location!r}')
        super().__init__(location, assume_yes)

    def from_clause(self, crawls):
        from cdx_toolkit.repackage.sources.duckdb import _build_from_clause

        if self.location.endswith('.parquet'):  # file(s), possibly a glob
            return f"read_parquet('{self.location}', hive_partitioning=true)", False
        # hive-partitioned index directory: prune the crawls in the glob
        return _build_from_clause(self.location, crawls), True

    def iter_sql_rows(self, sql):
        from cdx_toolkit.repackage.sources.duckdb import DuckDbSource

        source = DuckDbSource(query=sql)
        return source.iter_rows()


class AthenaBackend(SqlBackend):
    """The columnar index queried with AWS Athena.

    Location: athena://athena.<region>.amazonaws.com[:443]/<database>?S3OutputLocation=s3://...[&table=ccindex]
    """

    name = 'Athena'
    engine = 'athena'
    url_file_hint = (
        ' (after creating an Athena table `target_urls` from the URL file on S3):\n\n'
        '  SELECT idx.url, idx.warc_filename, idx.warc_record_offset, idx.warc_record_length\n'
        '  FROM ccindex AS idx\n'
        '  JOIN target_urls AS target ON idx.url = target.url\n'
        "  WHERE idx.crawl = 'CC-MAIN-2024-33' AND idx.subset = 'warc'"
    )

    def __init__(self, location: str, assume_yes: bool = False):
        super().__init__(location, assume_yes)
        self.region, self.database, self.table, self.s3_output_location = parse_athena_location(location)

    def from_clause(self, crawls):
        return self.table, False

    def iter_sql_rows(self, sql):
        from cdx_toolkit.repackage.sources.athena import AthenaSource

        source = AthenaSource(
            query=sql,
            database=self.database,
            s3_output_location=self.s3_output_location,
            warc_download_prefix=None,
            region_name=self.region,
        )
        return source.iter_rows()


def parse_athena_location(location: str) -> Tuple[str, str, str, str]:
    """athena://athena.us-east-1.amazonaws.com:443/ccindex?S3OutputLocation=s3://bucket/path/
    -> (region, database, table, S3 output location)"""
    parsed = urlparse(location)
    match = re.fullmatch(r'athena\.([a-z0-9-]+)\.amazonaws\.com', parsed.hostname or '')
    if parsed.scheme != 'athena' or not match:
        raise UsageError(
            f'invalid Athena location {location!r}, expected '
            'athena://athena.<region>.amazonaws.com/<database>?S3OutputLocation=s3://<bucket>/<path>/'
        )
    params = parse_qs(parsed.query)
    s3_output_location = params.get('S3OutputLocation', [None])[0]
    if not s3_output_location or not s3_output_location.startswith('s3://'):
        raise UsageError(f'the Athena location {location!r} needs a S3OutputLocation=s3://... parameter')
    database = parsed.path.strip('/') or 'ccindex'
    table = params.get('table', ['ccindex'])[0]
    return match.group(1), database, table, s3_output_location
