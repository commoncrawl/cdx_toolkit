import io
import json

import pytest

from cdx_toolkit.record_io import RecordWriter, format_from_path, is_glob, read_records, sniff_format, write_records


RECORDS = [
    {'urlkey': 'com,example)/', 'timestamp': '20240101000000', 'url': 'http://example.com/', 'status': '200',
     'filename': 'crawl-data/a.warc.gz', 'offset': '10', 'length': '20'},
    {'urlkey': 'com,example)/x', 'timestamp': '20240102000000', 'url': 'http://example.com/x', 'status': '404',
     'filename': 'crawl-data/b.warc.gz', 'offset': '30', 'length': '40'},
]


@pytest.mark.parametrize(
    'path, fmt',
    [
        ('-', None),
        ('out.jsonl', 'jsonl'),
        ('out.json.gz', 'jsonl'),
        ('s3://bucket/out.cdxj', 'cdxj'),
        ('out.cdx.gz', 'cdxj'),
        ('OUT.CSV', 'csv'),
        ('out.tsv.gz', 'tsv'),
        ('out.txt', None),
    ],
)
def test_format_from_path(path, fmt):
    assert format_from_path(path) == fmt


@pytest.mark.parametrize('fmt', ['jsonl', 'cdxj', 'csv', 'tsv'])
@pytest.mark.parametrize('extension', ['', '.gz'])
def test_round_trip(tmp_path, fmt, extension):
    path = str(tmp_path / f'records.{fmt}{extension}')
    assert write_records(iter(RECORDS), path, fmt) == 2

    # the format is detected from the content, not the extension
    assert list(read_records(path)) == RECORDS


def test_writer_fields():
    fh = io.StringIO()
    writer = RecordWriter(fh, 'csv', fields=['url', 'status', 'missing'])
    for record in RECORDS:
        writer.write(record)
    assert fh.getvalue().splitlines() == ['url,status,missing', 'http://example.com/,200,', 'http://example.com/x,404,']

    fh = io.StringIO()
    RecordWriter(fh, 'cdxj', fields=['url']).write(RECORDS[0])
    assert fh.getvalue() == 'com,example)/ 20240101000000 {"url": "http://example.com/"}\n'


def test_writer_cdxj_requires_urlkey():
    with pytest.raises(ValueError, match='urlkey'):
        RecordWriter(io.StringIO(), 'cdxj').write({'url': 'http://example.com/', 'timestamp': '2024'})


def test_read_columnar_index_names(tmp_path):
    path = tmp_path / 'range-jobs.csv'
    path.write_text('url,warc_filename,warc_record_offset,warc_record_length\nhttp://example.com/,a.warc.gz,1,2\n')
    assert list(read_records(str(path))) == [
        {'url': 'http://example.com/', 'filename': 'a.warc.gz', 'offset': '1', 'length': '2'}
    ]


def test_read_empty_and_stdin(tmp_path, monkeypatch):
    path = tmp_path / 'empty.jsonl'
    path.write_text('')
    assert list(read_records(str(path))) == []

    monkeypatch.setattr('sys.stdin', io.StringIO(json.dumps(RECORDS[0]) + '\n'))
    assert list(read_records('-')) == [RECORDS[0]]


@pytest.mark.parametrize(
    'line, fmt',
    [
        ('{"url": "http://example.com/"}', 'jsonl'),
        ('com,example)/ 20240101000000 {"url": "http://example.com/"}', 'cdxj'),
        ('url\tfilename\toffset\tlength', 'tsv'),
        ('url,filename,offset,length', 'csv'),
    ],
)
def test_sniff_format(line, fmt):
    assert sniff_format(line) == fmt


def test_parquet_round_trip(tmp_path):
    pytest.importorskip('pyarrow')
    path = str(tmp_path / 'records.parquet')
    assert format_from_path(path) == 'parquet'
    assert write_records(iter(RECORDS), path, 'parquet') == 2
    assert list(read_records(path)) == RECORDS

    # selected fields, missing values are left out when reading
    write_records(iter(RECORDS), path, 'parquet', fields=['url', 'status', 'missing'])
    assert list(read_records(path)) == [
        {'url': 'http://example.com/', 'status': '200'}, {'url': 'http://example.com/x', 'status': '404'},
    ]


def test_parquet_many_batches(tmp_path, monkeypatch):
    pytest.importorskip('pyarrow')
    import pyarrow.parquet

    monkeypatch.setattr('cdx_toolkit.record_io.PARQUET_BATCH_SIZE', 3)
    records = [{'url': f'http://example.com/{i}', 'offset': i} for i in range(7)]
    path = str(tmp_path / 'records.parquet')

    assert write_records(iter(records), path, 'parquet') == 7
    assert pyarrow.parquet.ParquetFile(path).metadata.num_row_groups == 3
    assert [r['offset'] for r in read_records(path)] == [str(i) for i in range(7)]  # all columns are strings


def test_parquet_needs_a_file(tmp_path):
    with pytest.raises(ValueError, match='stdout'):
        write_records(iter(RECORDS), '-', 'parquet')


def test_cdx_is_an_alias_of_cdxj(tmp_path):
    path = str(tmp_path / 'records.cdx')
    assert format_from_path(path) == 'cdxj'
    write_records(iter(RECORDS), path, 'cdx')
    assert open(path).read().startswith('com,example)/ 20240101000000 {')
    assert list(read_records(path, 'cdx')) == RECORDS


@pytest.mark.parametrize(
    'path, glob',
    [
        ('https://example.org/index.cdx.gz?signature=x', False),
        ('https://example.org/indexes/*.gz', True),
        ('s3://bucket/cdx-0000?.gz', True),
        ('-', False),
    ],
)
def test_is_glob(path, glob):
    assert is_glob(path) == glob


def test_no_globs_over_http():
    with pytest.raises(ValueError, match='globs do not work with http'):
        list(read_records('https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2024-30/indexes/cdx-*.gz'))
