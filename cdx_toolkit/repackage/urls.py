'''
The URL schemes of WARC files and outputs that need their own readers or writers: s3:// and hf://.
'''
from typing import Optional, Tuple

HF_SCHEME = 'hf://'


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse a S3 URI and return bucket and prefix."""
    if not uri.startswith('s3://'):
        raise ValueError(f'Not an S3 URI: {uri}')
    rest = uri[5:]
    i = rest.find('/')
    if i <= 0 or i == len(rest) - 1:
        raise ValueError(f'Malformed S3 URI: {uri}')
    return rest[:i], rest[i+1:]


def is_s3_url(url: Optional[str]) -> bool:
    return bool(url) and url.startswith('s3:/')


def is_hf_url(url) -> bool:
    """True for an ``hf://`` URL (e.g. an hf:// WARC download prefix or job URL)."""
    return bool(url) and url.startswith(HF_SCHEME)
