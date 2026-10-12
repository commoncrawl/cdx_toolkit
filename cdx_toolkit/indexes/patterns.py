'''
URL patterns (as for the CDX API) -> SURT keys and prefixes, used to select captures from CDX files
and from the columnar index (url_surtkey).
'''
from typing import List, Tuple

from .base import UsageError


def to_surt(url: str) -> str:
    from url_is_in import convert_url_to_surt

    return convert_url_to_surt(url)


def url_pattern_to_surts(pattern: str) -> Tuple[List[str], List[str]]:
    """A CDX API URL pattern -> (SURT prefixes, exact SURT keys).

    - `*.example.com`: the domain and its subdomains
    - `example.com/*`, `example.com/path*`: all URLs starting with the pattern
    - `example.com/path`: exactly this URL
    """
    pattern = pattern.strip()
    if pattern.startswith('*.'):
        domain = pattern[2:]
        if not domain or any(c in domain for c in '*/?'):
            raise UsageError(f'invalid URL pattern {pattern!r}, a domain pattern is e.g. *.example.com')
        base = to_surt(domain)[:-2]  # 'com,example)/' -> 'com,example'
        return [base + ')', base + ','], []
    if pattern.endswith('*'):
        prefix = pattern[:-1]
        if '*' in prefix:
            raise UsageError(f'invalid URL pattern {pattern!r}, * can only be at the start or the end')
        return [to_surt(prefix)], []
    if '*' in pattern:
        raise UsageError(f'invalid URL pattern {pattern!r}, * can only be at the start or the end')
    return [], [to_surt(pattern)]


def normalize_host(host: str) -> str:
    """A hostname or domain as in the index (url_host_name of the columnar index): lower case, no trailing dot."""
    return host.strip().lower().rstrip('.')


def url_hostname(url: str) -> str:
    """The (normalized) hostname of a URL, '' if it has none."""
    from urllib.parse import urlsplit

    if '://' not in url:
        url = 'http://' + url
    return normalize_host(urlsplit(url).hostname or '')


def hostname_to_surts(hostname: str) -> List[str]:
    """The URLs of this host, and of the host with or without www. (the SURT key drops www.); the exact host is
    checked with url_hostname()."""
    return [to_surt(normalize_host(hostname) + '/')]


def domain_to_surts(domain: str) -> List[str]:
    """All URLs of the domain and its subdomains."""
    return url_pattern_to_surts('*.' + normalize_host(domain))[0]


def domain_entry_to_surts(entry: str) -> List[str]:
    """A --url-file-type domain entry: a domain with its subdomains, or a URL prefix if it has a path."""
    entry = entry.strip()
    host_and_path = entry.split('://', 1)[-1]
    if '*' in entry:
        raise UsageError(f'invalid domain entry {entry!r} (no wildcards with --url-file-type domain)')
    if '/' not in host_and_path.rstrip('/') and '?' not in host_and_path:
        return domain_to_surts(host_and_path.rstrip('/'))
    return [to_surt(entry)]
