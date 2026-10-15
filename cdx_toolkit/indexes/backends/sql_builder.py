'''
SQL text for the columnar URL index (Athena, DuckDB): escaping, the WHERE clause of the guided queries, crawls.
'''
import datetime
import logging
import re
from typing import List, Optional, Tuple

from cdx_toolkit.commoncrawl import normalize_crawl, get_cc_endpoints, match_cc_crawls
from cdx_toolkit.timeutils import pad_timestamp, pad_timestamp_up

from ..base import UsageError
from ..patterns import domain_to_surts, normalize_host, url_pattern_to_surts
from ..query import IndexQuery


logger = logging.getLogger(__name__)

# Hostnames, TLDs and crawl names only ever contain these characters. Validating
# against this set both prevents SQL injection and catches malformed input early.
_SQL_LITERAL_RE = re.compile(r'^[A-Za-z0-9.\-]+$')

# Default Common Crawl index mirror used to resolve --crawl to concrete crawl names.
CC_INDEX_MIRROR = 'https://index.commoncrawl.org/'


def escape_sql_literal(value: str) -> str:
    """Validate and quote a value for safe inclusion in a SQL string literal.

    Only hostname/TLD/crawl-name characters (letters, digits, dot, hyphen) are
    allowed, so the result cannot break out of the quotes or inject SQL."""
    if not isinstance(value, str) or not _SQL_LITERAL_RE.match(value):
        raise ValueError(
            f'Invalid value for SQL query literal: {value!r} '
            '(allowed characters: letters, digits, dot, hyphen)'
        )
    return "'" + value + "'"


def quote_sql_string(value: str) -> str:
    """Quote any string for a SQL string literal (single quotes are doubled)."""
    if any(ord(c) < 32 for c in value):
        raise ValueError(f'Invalid value for SQL query literal: {value!r} (control characters)')
    return "'" + value.replace("'", "''") + "'"


def sql_like_prefix(prefix: str) -> str:
    """LIKE pattern (with ESCAPE '\\') matching all strings that start with `prefix`."""
    escaped = prefix.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    return f"LIKE {quote_sql_string(escaped + '%')} ESCAPE '\\'"


def surt_tld(surt: str) -> str:
    """'com,example)/path' -> 'com'"""
    return re.split('[,):]', surt, maxsplit=1)[0]


def reversed_host(host: str) -> Optional[str]:
    """The url_host_name_reversed of a host (e.g. www.example.com -> com.example.www), None for an IP address or
    a host with other characters than letters, digits, dot and hyphen (these get no url_host_name_reversed
    predicates)."""
    labels = host.split('.')
    if not _SQL_LITERAL_RE.match(host) or not all(labels) or all(label.isdigit() for label in labels):
        return None
    return '.'.join(reversed(labels))


def reversed_host_range(reversed_prefix: str) -> str:
    """url_host_name_reversed values starting with the reversed host: the host and its subdomains (and a few more
    hosts, e.g. com.example-shop for com.example). A range, not a LIKE: the engines skip the row groups outside it
    by their min/max statistics (the index is sorted by url_surtkey, which follows url_host_name_reversed)."""
    escape_sql_literal(reversed_prefix)  # validates
    return f"url_host_name_reversed >= '{reversed_prefix}' AND url_host_name_reversed < '{reversed_prefix}/'"


def surt_host_hint(surt: str) -> Optional[str]:
    """The url_host_name_reversed range of the hosts of a SURT key or prefix (e.g. 'com,example)/path' or
    'com,example,' or 'com,example:8080)/'), None if it has no plain host (an IP address)."""
    host = re.split('[):]', surt, maxsplit=1)[0].rstrip(',')
    reversed_prefix = reversed_host('.'.join(reversed(host.split(',')))) if host else None
    return reversed_host_range(reversed_prefix) if reversed_prefix else None


def hostname_predicate(hostname: str) -> str:
    """Captures of the exact host: url_host_name, and url_host_name_reversed for the min/max statistics (see
    reversed_host_range)."""
    predicate = f'url_host_name = {escape_sql_literal(hostname)}'
    reversed_name = reversed_host(hostname)
    if reversed_name is None:
        return predicate
    return f"(url_host_name_reversed = '{reversed_name}' AND {predicate})"


def domain_predicate(domain: str) -> str:
    """Captures of the domain and its subdomains (like --domain for CDX files), e.g. for blog.example.com:

        url_host_name_reversed >= 'com.example.blog' AND url_host_name_reversed < 'com.example.blog/'
        AND (url_host_name_reversed = 'com.example.blog' OR url_host_name_reversed LIKE 'com.example.blog.%')

    The range lets the engines skip the row groups of other hosts (see reversed_host_range), the small
    dictionary-encoded url_host_name_reversed gives the exact host boundary. An IP address is matched by its
    url_surtkey prefixes and registered domain instead."""
    surts = domain_to_surts(domain)
    labels = surts[0][:-1].split(',')[::-1]  # canonical, e.g. without www.
    reversed_domain = reversed_host('.'.join(labels))
    if reversed_domain is not None:
        return (f'({reversed_host_range(reversed_domain)} AND (url_host_name_reversed = \'{reversed_domain}\' '
                f"OR url_host_name_reversed LIKE '{reversed_domain}.%'))")
    suffixes = ['.'.join(labels[i:]) for i in range(len(labels) - 1)] or labels
    registered = ', '.join(escape_sql_literal(suffix) for suffix in suffixes)
    surt_match = ' OR '.join(f'url_surtkey {sql_like_prefix(surt)}' for surt in surts)
    return f'(url_host_registered_domain IN ({registered}) AND ({surt_match}))'


def surt_predicate(surt: str, prefix: bool) -> str:
    """Captures of a url_surtkey (prefix), with the url_host_name_reversed range of its host for the min/max
    statistics (see surt_host_hint)."""
    predicate = f'url_surtkey {sql_like_prefix(surt)}' if prefix else f'url_surtkey = {quote_sql_string(surt)}'
    hint = surt_host_hint(surt)
    return f'({hint} AND {predicate})' if hint else predicate


def build_where_sql(query: IndexQuery, crawls: Optional[List[str]] = None) -> str:
    """Build the WHERE body (without the `WHERE` keyword) of a guided query, shared by all SQL engines.

    The URL selectors are OR-ed: --hostname matches `url_host_name` (exact host, e.g. www.example.com),
    --domain the domain with its subdomains (see domain_predicate), --url the `url_surtkey` (SURT prefixes or
    exact keys). At least one of them is required. If `crawls` is a non-empty list of crawl names (e.g.
    ['CC-MAIN-2025-33']), a `crawl IN (...)` partition filter is added -- the main lever for reducing scan cost.
    --from, --to and --language are AND-ed (see query_conditions). Engines differ only in their FROM clause (see
    build_sql)."""
    hostnames = [normalize_host(h) for h in query.hostnames]
    domains = [normalize_host(d) for d in query.domains]
    surt_prefixes, surts = url_surtkeys(query)
    if not hostnames and not domains and not surt_prefixes and not surts:
        raise ValueError('an index query requires at least one hostname, registered domain or URL')

    tlds = {v.split('.')[-1] for v in hostnames + domains}
    tlds |= {surt_tld(s) for s in surt_prefixes + surts}
    query_tlds = ' OR '.join(f'url_host_tld = {escape_sql_literal(t)}' for t in sorted(tlds))

    host_predicates = [hostname_predicate(h) for h in hostnames]
    host_predicates += [domain_predicate(d) for d in domains]
    host_predicates += [surt_predicate(s, prefix=False) for s in surts]
    host_predicates += [surt_predicate(s, prefix=True) for s in surt_prefixes]
    query_hosts = ' OR '.join(host_predicates)

    clauses = [
        "subset = 'warc'",
        f'({query_tlds}) -- help the query optimizer',
        f'({query_hosts})',
    ]
    if crawls:
        crawl_in = ', '.join(escape_sql_literal(c) for c in crawls)
        clauses.append(f'crawl IN ({crawl_in})')
    clauses.extend(query_conditions(query))

    return '\n        AND '.join(clauses)


# Sorting range jobs by (warc_filename, warc_record_offset) groups records of the
# same WARC file with ascending offsets, which improves S3 range-read locality (and
# enables coalescing adjacent ranges) at fetch time.
ORDER_BY_SQL = 'ORDER BY warc_filename, warc_record_offset'


def build_sql(
    query: IndexQuery,
    from_clause: str,
    crawls: Optional[List[str]] = None,
    order_by: bool = True,
    columns: Optional[List[str]] = None,
) -> str:
    """The SELECT of a guided query (--url, --hostname, --domain, ...) of the columnar index.

    `from_clause` is the text following FROM (e.g. `ccindex` for Athena, or a `read_parquet(...)` expression for
    DuckDB); `crawls` adds a partition filter (see build_where_sql). When `order_by` is set, results are sorted by
    (warc_filename, warc_record_offset) for fetch-time read locality. `columns` defaults to the WARC location
    columns."""
    where_sql = build_where_sql(query, crawls)
    order_sql = f'\n    {ORDER_BY_SQL}' if order_by else ''
    limit_sql = f'\n    LIMIT {query.limit}' if query.limit and query.limit > 0 else ''
    columns_sql = ', '.join(columns or ('warc_filename', 'warc_record_offset', 'warc_record_length'))

    return f"""
    SELECT
        {columns_sql}
    FROM {from_clause}
    WHERE {where_sql}{order_sql}{limit_sql}"""


def endpoint_to_crawl_name(endpoint: str) -> str:
    """Turn a collinfo cdx-api endpoint into its crawl name.

    e.g. 'https://index.commoncrawl.org/CC-MAIN-2025-33-index' -> 'CC-MAIN-2025-33'."""
    name = endpoint.rstrip('/').split('/')[-1]
    if name.endswith('-index'):
        name = name[:-len('-index')]
    return name


def resolve_crawl_names(crawl_arg) -> List[str]:
    """Resolve a --crawl value to concrete CC-MAIN crawl names for the partition filter.

    Reuses the CDX path's helpers so `--crawl` accepts the same forms (comma-separated
    names or an integer for the most recent N crawls)."""
    crawls = normalize_crawl([crawl_arg])
    raw_index_list = get_cc_endpoints(CC_INDEX_MIRROR)
    matched = match_cc_crawls(crawls, raw_index_list)
    return [endpoint_to_crawl_name(ep) for ep in matched]


def sql_timestamp(timestamp: str, end: bool = False) -> str:
    """CDX timestamp (e.g. 2008, 200906302359) -> SQL TIMESTAMP literal of its first second, or with `end`, of the
    second after its last second (an exclusive end, which includes the fractions of the last second)."""
    if not re.fullmatch(r'\d{4,14}', timestamp):
        raise UsageError(f'invalid timestamp {timestamp!r}, use e.g. 2008, 20080328 or 200906302359')
    padded = pad_timestamp_up(timestamp) if end else pad_timestamp(timestamp)
    try:
        moment = datetime.datetime.strptime(padded, '%Y%m%d%H%M%S')
        if end:
            moment += datetime.timedelta(seconds=1)
    except (ValueError, OverflowError):
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
        conditions.append(f'fetch_time < {sql_timestamp(query.to, end=True)}')
    if query.languages:
        # primary content language: the first of the comma-separated content_languages
        predicates = []
        for code in query.languages:
            language = check_language(code)
            predicates += [f"content_languages = '{language}'", f"content_languages LIKE '{language},%'"]
        conditions.append('(' + ' OR '.join(predicates) + ')')
    return conditions
