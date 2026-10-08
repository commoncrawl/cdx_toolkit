import csv

import pytest

from cdx_toolkit.repackage.data_classes import RangeJob
from cdx_toolkit.repackage.sources.csv import RangeJobCsvWriter, CsvSource


def test_writer_writes_full_urls_and_reads_them_back(tmp_path):
    path = str(tmp_path / 'ranges.csv')
    w = RangeJobCsvWriter(path)
    w.write(RangeJob(url='s3://commoncrawl/crawl-data/x.warc.gz', offset=5, length=7, filename='crawl-data/x.warc.gz'))
    w.close()

    with open(path, newline='') as f:
        rows = list(csv.DictReader(f))
    assert rows[0] == {
        'warc_filename': 's3://commoncrawl/crawl-data/x.warc.gz',
        'warc_record_offset': '5',
        'warc_record_length': '7',
    }

    # a full URL is used as-is, whatever the prefix
    jobs = list(CsvSource(path, 'https://ignored').iter_range_jobs())
    assert jobs[0].url == 's3://commoncrawl/crawl-data/x.warc.gz'
    assert jobs[0].offset == 5 and jobs[0].length == 7


def test_csv_source_relative_filename_gets_prefix(tmp_path):
    path = str(tmp_path / 'ranges.csv')
    with open(path, 'w') as f:
        f.write('warc_filename,warc_record_offset,warc_record_length\n')
        f.write('crawl-data/x.warc.gz,10,20\n')
        f.write('https://example.org/y.warc.gz,1,2\n')
    jobs = list(CsvSource(path, 'https://data.commoncrawl.org', sort=False).iter_range_jobs())
    assert [j.url for j in jobs] == [
        'https://data.commoncrawl.org/crawl-data/x.warc.gz', 'https://example.org/y.warc.gz',
    ]
    assert jobs[0].filename == 'crawl-data/x.warc.gz'


def test_writer_extra_columns_round_trip(tmp_path):
    path = str(tmp_path / 'ranges.csv')
    w = RangeJobCsvWriter(path)
    w.write(RangeJob(
        url='https://data.commoncrawl.org/crawl-data/x.warc.gz',
        offset=10, length=20, filename='crawl-data/x.warc.gz',
        extra={'content_languages': 'eng', 'url': 'https://example.com/page'},
    ))
    w.close()

    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == [
            'warc_filename', 'warc_record_offset', 'warc_record_length',
            'content_languages', 'url',
        ]
        rows = list(reader)
    assert rows[0]['content_languages'] == 'eng'
    assert rows[0]['url'] == 'https://example.com/page'

    # extras round-trip back onto RangeJob.extra
    jobs = list(CsvSource(path, None).iter_range_jobs())
    assert jobs[0].extra == {'content_languages': 'eng', 'url': 'https://example.com/page'}
    assert jobs[0].url == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'


def test_writer_empty_writes_base_header(tmp_path):
    path = str(tmp_path / 'empty.csv')
    w = RangeJobCsvWriter(path)
    w.close()
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert reader.fieldnames == ['warc_filename', 'warc_record_offset', 'warc_record_length']
    assert rows == []


def test_csv_source_warc_url_column_is_not_supported(tmp_path):
    path = str(tmp_path / 'old.csv')
    with open(path, 'w') as f:
        f.write('warc_url,warc_record_offset,warc_record_length\n')
        f.write('s3://commoncrawl/crawl-data/x.warc.gz,5,7\n')
    with pytest.raises(ValueError, match='warc_filename'):
        list(CsvSource(path, 'https://x').iter_range_jobs())


def test_csv_source_missing_columns(tmp_path):
    path = str(tmp_path / 'bad.csv')
    with open(path, 'w') as f:
        f.write('foo,bar\n1,2\n')
    with pytest.raises(ValueError):
        list(CsvSource(path, 'https://x').iter_range_jobs())


def test_csv_source_tsv(tmp_path):
    path = str(tmp_path / 'ranges.tsv')
    with open(path, 'w') as f:
        f.write('warc_filename\twarc_record_offset\twarc_record_length\n')
        f.write('crawl-data/x.warc.gz\t10\t20\n')
    jobs = list(CsvSource(path, 'https://data.commoncrawl.org').iter_range_jobs())
    assert jobs[0].url == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    assert jobs[0].offset == 10 and jobs[0].length == 20


def test_csv_source_sorts_by_filename_then_offset(tmp_path):
    path = str(tmp_path / 'ranges.csv')
    with open(path, 'w') as f:
        f.write('warc_filename,warc_record_offset,warc_record_length\n')
        # deliberately out of order: file b before a, and offsets descending
        f.write('b.warc.gz,50,1\n')
        f.write('a.warc.gz,200,1\n')
        f.write('a.warc.gz,10,1\n')
    jobs = list(CsvSource(path, 'https://data.commoncrawl.org').iter_range_jobs())
    assert [(j.filename, j.offset) for j in jobs] == [
        ('a.warc.gz', 10), ('a.warc.gz', 200), ('b.warc.gz', 50),
    ]


def test_csv_source_no_sort_preserves_order(tmp_path):
    path = str(tmp_path / 'ranges.csv')
    with open(path, 'w') as f:
        f.write('warc_filename,warc_record_offset,warc_record_length\n')
        f.write('b.warc.gz,50,1\n')
        f.write('a.warc.gz,200,1\n')
        f.write('a.warc.gz,10,1\n')
    jobs = list(CsvSource(path, 'https://data.commoncrawl.org', sort=False).iter_range_jobs())
    assert [(j.filename, j.offset) for j in jobs] == [
        ('b.warc.gz', 50), ('a.warc.gz', 200), ('a.warc.gz', 10),
    ]
