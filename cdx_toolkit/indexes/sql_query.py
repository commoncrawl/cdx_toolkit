'''
SQL queries for the columnar URL index (Athena, DuckDB): escaping, the WHERE clause of the guided queries, crawls.
'''
import logging
import re
from typing import List, Optional

from cdx_toolkit.commoncrawl import normalize_crawl, get_cc_endpoints, match_cc_crawls

from .patterns import domain_to_surts, normalize_host


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
    return surt.split(',', 1)[0].split(')', 1)[0]


def domain_predicate(domain: str) -> str:
    """Captures of the domain and its subdomains (like --domain for CDX files), e.g. for blog.example.com:

        url_host_registered_domain IN ('blog.example.com', 'example.com')
        AND (url_surtkey LIKE 'com,example,blog)%' OR url_surtkey LIKE 'com,example,blog,%')

    The SURT prefixes give the exact host boundary; the registered domain, which is one of the suffixes of the
    domain, makes the query cheaper (an equality the engines can push down)."""
    surts = domain_to_surts(domain)
    labels = surts[0][:-1].split(',')[::-1]  # canonical, e.g. without www.
    suffixes = ['.'.join(labels[i:]) for i in range(len(labels) - 1)] or labels
    registered = ', '.join(escape_sql_literal(suffix) for suffix in suffixes)
    surt_match = ' OR '.join(f'url_surtkey {sql_like_prefix(surt)}' for surt in surts)
    return f'(url_host_registered_domain IN ({registered}) AND ({surt_match}))'


def build_where_sql(
    url_host_names: Optional[List[str]] = None,
    crawls: Optional[List[str]] = None,
    url_host_registered_domains: Optional[List[str]] = None,
    conditions: Optional[List[str]] = None,
    url_surtkey_prefixes: Optional[List[str]] = None,
    url_surtkeys: Optional[List[str]] = None,
) -> str:
    """Build the WHERE body (without the `WHERE` keyword) shared by all SQL engines.

    The guided filter matches on `url_host_name` (exact host, e.g. www.example.com),
    domains with their subdomains (see domain_predicate) and/or `url_surtkey` (SURT keys
    of URL patterns: `url_surtkey_prefixes` match all keys starting with them,
    `url_surtkeys` exactly); predicates are OR-ed together.
    At least one host, domain or SURT key is required. If `crawls` is a non-empty list
    of crawl names (e.g. ['CC-MAIN-2025-33']), a `crawl IN (...)` partition filter is
    added -- the main lever for reducing scan cost. `conditions` are further (already
    escaped) predicates that are AND-ed. Engines differ only in their FROM clause (see
    build_sql)."""
    url_host_names = [normalize_host(h) for h in url_host_names or []]
    domains = [normalize_host(d) for d in url_host_registered_domains or []]
    surt_prefixes = url_surtkey_prefixes or []
    surts = url_surtkeys or []
    if not url_host_names and not domains and not surt_prefixes and not surts:
        raise ValueError('an index query requires at least one hostname, registered domain or URL')

    tlds = {v.split('.')[-1] for v in (list(url_host_names) + list(domains))}
    tlds |= {surt_tld(s) for s in surt_prefixes + surts}
    query_tlds = ' OR '.join(f'url_host_tld = {escape_sql_literal(t)}' for t in sorted(tlds))

    host_predicates = [f'url_host_name = {escape_sql_literal(h)}' for h in url_host_names]
    host_predicates += [domain_predicate(d) for d in domains]
    host_predicates += [f'url_surtkey = {quote_sql_string(s)}' for s in surts]
    host_predicates += [f'url_surtkey {sql_like_prefix(s)}' for s in surt_prefixes]
    query_hosts = ' OR '.join(host_predicates)

    clauses = [
        "subset = 'warc'",
        f'({query_tlds}) -- help the query optimizer',
        f'({query_hosts})',
    ]
    if crawls:
        crawl_in = ', '.join(escape_sql_literal(c) for c in crawls)
        clauses.append(f'crawl IN ({crawl_in})')
    clauses.extend(conditions or [])

    return '\n        AND '.join(clauses)


# Sorting range jobs by (warc_filename, warc_record_offset) groups records of the
# same WARC file with ascending offsets, which improves S3 range-read locality (and
# enables coalescing adjacent ranges) at fetch time.
ORDER_BY_SQL = 'ORDER BY warc_filename, warc_record_offset'


def build_sql(
    from_clause: str,
    url_host_names: Optional[List[str]] = None,
    crawls: Optional[List[str]] = None,
    limit: int = 0,
    url_host_registered_domains: Optional[List[str]] = None,
    order_by: bool = True,
    columns: Optional[List[str]] = None,
    conditions: Optional[List[str]] = None,
    url_surtkey_prefixes: Optional[List[str]] = None,
    url_surtkeys: Optional[List[str]] = None,
) -> str:
    """Assemble a full SELECT for the columnar index.

    `from_clause` is the text following FROM (e.g. `ccindex` for Athena, or a
    `read_parquet(...)` expression for DuckDB). When `order_by` is set, results are
    sorted by (warc_filename, warc_record_offset) for fetch-time read locality.
    `columns` defaults to the WARC location columns."""
    where_sql = build_where_sql(
        url_host_names, crawls, url_host_registered_domains=url_host_registered_domains, conditions=conditions,
        url_surtkey_prefixes=url_surtkey_prefixes, url_surtkeys=url_surtkeys,
    )
    order_sql = f'\n    {ORDER_BY_SQL}' if order_by else ''
    limit_sql = f'\n    LIMIT {limit}' if limit and limit > 0 else ''
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
