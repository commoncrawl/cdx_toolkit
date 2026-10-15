"""Live tests of the Hugging Face Storage Buckets (CDXT_LIVE_TESTS=hf): `cdxt index cc-url-index-hf`, and
`cdxt repackage` reading from hf://buckets/commoncrawl/commoncrawl and writing to TEST_HF_BUCKET.

DuckDB reads of the columnar index over hf:// are slow (minutes for a single parquet file, mostly the latency
of the Hub API), so the index test reads a single file with LIMIT, and the repackage tests use known captures.
"""
import json
import os

import fsspec
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.cli import main
from tests.conftest import requires_duckdb, requires_live_hf


HF_PREFIX = 'hf://buckets/commoncrawl/commoncrawl'
CRAWL = 'CC-MAIN-2026-17'
INDEX_FILE = (f'{HF_PREFIX}/cc-index/table/cc-main/warc/crawl={CRAWL}/subset=warc/'
              'part-00000-ee7620e8-27a7-4c74-846b-a92b039dae92.c000.gz.parquet')

# captures of CC-MAIN-2026-17 (warc and crawldiagnostics files), from the CDX API
CAPTURES = [
    {'url': 'https://commoncrawl.org/', 'timestamp': '20260410082607', 'status': '200',
     'digest': 'ZVJ34E3JKBK2AXINWBT3QWHBDNYZU32Z', 'offset': '133904328', 'length': '7487',
     'filename': 'crawl-data/CC-MAIN-2026-17/segments/1775805908305.14/warc/'
                 'CC-MAIN-20260410081153-20260410111153-00389.warc.gz'},
    {'url': 'https://www.commoncrawl.org/', 'timestamp': '20260413052322', 'status': '200',
     'digest': 'CAZVHFEZWLAZELACXKKCVDGSKTUQCRJ7', 'offset': '597179815', 'length': '7496',
     'filename': 'crawl-data/CC-MAIN-2026-17/segments/1775805908721.20/warc/'
                 'CC-MAIN-20260413040425-20260413070425-00340.warc.gz'},
    {'url': 'http://commoncrawl.org/', 'timestamp': '20260410082607', 'status': '301',
     'digest': 'LGGT76I5HYSS6HQT36GPANELE4H7FWR7', 'offset': '157143', 'length': '877',
     'filename': 'crawl-data/CC-MAIN-2026-17/segments/1775805908305.14/crawldiagnostics/'
                 'CC-MAIN-20260410081153-20260410111153-00494.warc.gz'},
]


def _write_captures(tmpdir):
    path = os.path.join(str(tmpdir), 'captures.jsonl')
    with open(path, 'w') as f:
        for capture in CAPTURES:
            f.write(json.dumps(capture) + '\n')
    return path


def _responses(path):
    """(WARC-Target-URI, WARC-Payload-Digest) of the response records of a WARC file (sorted: the records are
    written in the order of their WARC files and offsets)."""
    with fsspec.open(path, 'rb') as stream:
        return sorted((record.rec_headers.get_header('WARC-Target-URI'),
                       record.rec_headers.get_header('WARC-Payload-Digest'))
                      for record in ArchiveIterator(stream) if record.rec_type == 'response')


def _expected():
    return sorted((c['url'], 'sha1:' + c['digest']) for c in CAPTURES)


@requires_live_hf
@requires_duckdb
def test_index_cc_url_index_hf_sql(tmpdir):
    sql = os.path.join(str(tmpdir), 'query.sql')
    with open(sql, 'w') as f:
        f.write(f"""
            SELECT url, warc_filename, warc_record_offset, warc_record_length
            FROM read_parquet('{INDEX_FILE}')
            LIMIT 3
        """)
    captures_path = os.path.join(str(tmpdir), 'captures.jsonl')
    main(args=['index', 'cc-url-index-hf', '--sql', sql, '--yes', '-o', captures_path])

    with open(captures_path) as f:
        captures = [json.loads(line) for line in f]
    assert len(captures) == 3
    assert all(c['filename'].startswith(f'crawl-data/{CRAWL}/') for c in captures)
    assert all(int(c['length']) > 0 for c in captures)


@requires_live_hf
def test_repackage_from_hf(tmpdir):
    main(args=['repackage', _write_captures(tmpdir), f'--prefix={tmpdir}/TEST_hf', '--warc-download-prefix',
               HF_PREFIX])

    assert _responses(os.path.join(str(tmpdir), 'TEST_hf-001.warc.gz')) == _expected()


@requires_live_hf
def test_repackage_to_hf_bucket(tmpdir, hf_tmpdir):
    main(args=['repackage', _write_captures(tmpdir), f'--prefix={hf_tmpdir}/TEST_hf', '--warc-download-prefix',
               HF_PREFIX])

    assert _responses(f'{hf_tmpdir}/TEST_hf-001.warc.gz') == _expected()


@requires_live_hf
def test_repackage_from_hf_parallel_readers(tmpdir):
    main(args=['repackage', _write_captures(tmpdir), f'--prefix={tmpdir}/TEST_hf', '--warc-download-prefix',
               HF_PREFIX, '--parallel-readers', '3'])

    assert _responses(os.path.join(str(tmpdir), 'TEST_hf-001.warc.gz')) == _expected()
