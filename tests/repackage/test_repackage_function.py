"""repackage(), the Python equivalent of `cdxt repackage`, with open_index() and IndexQuery for `cdxt index`."""
import json
from unittest.mock import patch

import pytest
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.indexes import IndexQuery, UsageError, open_index
from cdx_toolkit.repackage import repackage
from cdx_toolkit.repackage.jobs import RepackageResult
from tests.fake_warc_server import FakeWarcServer


SERVER = FakeWarcServer('https://data.example.org')
CAPTURES = SERVER.add_captures('crawl-data/0.warc.gz', [f'https://example.com/{i}' for i in range(3)])


def warc_records(path):
    with open(path, 'rb') as f:
        return [(r.rec_type, r.rec_headers.get_header('WARC-Target-URI')) for r in ArchiveIterator(f)]


def fetch_from_server():
    return patch('cdx_toolkit.repackage.readers.http.myrequests_get', side_effect=SERVER.get)


def test_repackage_records(tmp_path):
    with fetch_from_server():
        result = repackage(CAPTURES, str(tmp_path / 'out'), warc_download_prefix='https://data.example.org')

    assert result == RepackageResult(requested=3, written=3)
    assert warc_records(tmp_path / 'out-001.warc.gz') == [('warcinfo', None)] + [
        ('resource', c['url']) for c in CAPTURES]


def test_repackage_index_file(tmp_path):
    path = tmp_path / 'captures.jsonl'
    path.write_text(''.join(json.dumps(c) + '\n' for c in CAPTURES))
    with fetch_from_server():
        result = repackage(str(path), str(tmp_path / 'out'), warc_download_prefix='https://data.example.org',
                           subprefix='SUB', creator='me')

    assert result.written == 3
    with open(tmp_path / 'out-SUB-001.warc.gz', 'rb') as f:
        warcinfo = next(iter(ArchiveIterator(f)))
    fields = warcinfo.content_stream().read().decode()
    assert 'isPartOf: ' + str(tmp_path / 'out') + '-SUB' in fields
    assert 'creator: me' in fields
    assert 'description: warc extraction generated with cdx_toolkit' in fields


def test_open_index_and_repackage(tmp_path):
    """The two steps in Python, as `cdxt index INDEX --domain example.com | cdxt repackage`."""
    index = tmp_path / 'index.cdx'
    index.write_text(''.join(
        f'com,example)/{c["url"].rsplit("/", 1)[1]} 20240101000000 {json.dumps(c)}\n' for c in CAPTURES))

    captures = open_index(str(index)).records(IndexQuery(domains='example.com', limit=2))
    with fetch_from_server():
        result = repackage(captures, str(tmp_path / 'out'), warc_download_prefix='https://data.example.org')

    assert result == RepackageResult(requested=2, written=2)
    assert [url for _, url in warc_records(tmp_path / 'out-001.warc.gz')[1:]] == [c['url'] for c in CAPTURES[:2]]


def test_repackage_usage_errors(tmp_path):
    (tmp_path / 'out-001.warc.gz').write_bytes(b'earlier run')
    with pytest.raises(UsageError, match='exist already'):
        repackage(CAPTURES, str(tmp_path / 'out'))
    with pytest.raises(UsageError, match='--processes cannot be used with --wb'):
        repackage(CAPTURES, str(tmp_path / 'new'), wb='ia', processes=2)
    with pytest.raises(UsageError, match='no WARC location'):
        repackage([{'url': 'https://example.com/'}], str(tmp_path / 'new'))
