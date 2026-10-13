"""WARC files served by fake HTTP range requests, with realistic responses (206, Content-Range, exact bytes)."""
from io import BytesIO
from typing import Dict, List
from unittest.mock import Mock

from warcio import WARCWriter

from cdx_toolkit.repackage.warc_records import get_bytes_from_warc_record


def warc_record_bytes(url: str) -> bytes:
    record = WARCWriter(None).create_warc_record(url, 'resource', payload=BytesIO(b'content of ' + url.encode()))
    return get_bytes_from_warc_record(record, gzip=True)


class FakeWarcServer:
    """WARC files (filename -> bytes) served under `prefix`; `get` stands in for myrequests_get."""

    def __init__(self, prefix: str = 'https://data.example.org'):
        self.prefix = prefix
        self.files: Dict[str, bytes] = {}
        self.requests: List[str] = []

    def add_captures(self, filename: str, urls: List[str]) -> List[dict]:
        """Append a WARC record per URL to the file, return the captures (CDX fields) of the records."""
        captures = []
        for url in urls:
            data = warc_record_bytes(url)
            offset = len(self.files.get(filename, b''))
            self.files[filename] = self.files.get(filename, b'') + data
            captures.append({'url': url, 'filename': filename, 'offset': str(offset), 'length': str(len(data))})
        return captures

    def get(self, url, headers=None, **kwargs):
        self.requests.append(url)
        data = self.files.get(url[len(self.prefix):].lstrip('/'))
        if data is None:
            return Mock(status_code=404, headers={}, content=b'not found')
        start, end = (int(n) for n in headers['Range'][len('bytes='):].split('-'))
        body = data[start:end + 1]
        content_range = f'bytes {start}-{start + len(body) - 1}/{len(data)}'
        return Mock(status_code=206, headers={'Content-Range': content_range}, content=body)
