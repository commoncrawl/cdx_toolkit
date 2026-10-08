from dataclasses import dataclass, field, fields
from typing import List, Optional, Set, Tuple


# command line flags of the query fields, if they differ from --<field-name>
FIELD_FLAGS = {
    'urls': '--url',
    'hostnames': '--hostname',
    'domains': '--domain',
    'from_ts': '--from',
    'filters': '--filter',
    'languages': '--language',
}


def field_flag(name: str) -> str:
    return FIELD_FLAGS.get(name, '--' + name.replace('_', '-'))


@dataclass
class IndexQuery:
    """What to select from an index, independent of the index backend.

    URL selectors (urls, hostnames, domains, url_file) are OR-ed; all other options are AND-ed.
    `sql` is a complete query for SQL backends, used instead of the other selectors.
    """

    urls: List[str] = field(default_factory=list)
    hostnames: List[str] = field(default_factory=list)
    domains: List[str] = field(default_factory=list)
    url_file: Optional[str] = None
    url_column: Optional[str] = None
    url_file_type: Optional[str] = None  # 'pattern' (default) or 'domain'
    crawl: Optional[str] = None
    from_ts: Optional[str] = None
    to: Optional[str] = None
    closest: Optional[str] = None
    filters: List[str] = field(default_factory=list)
    url_fgrep: Optional[str] = None
    url_fgrepv: Optional[str] = None
    limit: Optional[int] = None
    languages: List[str] = field(default_factory=list)
    sql: Optional[str] = None

    def url_patterns(self) -> List[Tuple[str, Optional[str]]]:
        """All URL selectors as (pattern, CDX API matchType) pairs."""
        return (
            [(url, None) for url in self.urls]
            + [(hostname, 'host') for hostname in self.hostnames]
            + [(domain, 'domain') for domain in self.domains]
        )

    def options_used(self) -> Set[str]:
        """Names of the options that are set, used to check what a backend supports."""
        return {f.name for f in fields(self) if getattr(self, f.name) not in (None, [])}
