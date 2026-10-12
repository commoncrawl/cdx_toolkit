import re
from typing import Optional


class RangeError(OSError):
    """A range request returned other bytes than requested."""


def check_range_response(status: int, content_range: Optional[str], body_length: int, offset: int, length: int,
                         url: str) -> None:
    """Raise RangeError unless an HTTP response holds exactly the bytes offset .. offset+length-1.

    A 206 response must name the requested range in Content-Range. A 200 response (the server ignored the
    range) is only accepted for a range from offset 0 that is the whole file. Redirects are not followed:
    a URL that redirects must be given as the redirect target."""
    end = offset + length - 1
    if status == 206:
        match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+|\*)', (content_range or '').strip())
        if not match or (int(match.group(1)), int(match.group(2))) != (offset, end):
            raise RangeError(f'{url}: requested bytes {offset}-{end}, got Content-Range {content_range!r}')
    elif status == 200:
        if offset != 0:
            raise RangeError(f'{url}: the server ignored the range request for bytes {offset}-{end}')
    else:
        raise RangeError(f'{url}: unexpected status {status} for the range request for bytes {offset}-{end}')
    if body_length != length:
        raise RangeError(f'{url}: requested {length} bytes from offset {offset}, got {body_length}')
