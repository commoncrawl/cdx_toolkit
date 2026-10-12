from cdx_toolkit.repackage.records_source import join_warc_url


def test_join_warc_url_relative():
    assert (
        join_warc_url('https://data.commoncrawl.org', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )


def test_join_warc_url_no_double_slash():
    assert (
        join_warc_url('https://data.commoncrawl.org/', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )
    assert join_warc_url('s3://bucket', '/crawl-data/x.warc.gz') == 's3://bucket/crawl-data/x.warc.gz'


def test_join_warc_url_empty_prefix():
    assert join_warc_url('', 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'
    assert join_warc_url(None, 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'


def test_join_warc_url_absolute_filename_passthrough():
    assert join_warc_url('https://data.commoncrawl.org', 's3://cc/x.warc.gz') == 's3://cc/x.warc.gz'
    assert join_warc_url('', 'https://host/x.warc.gz') == 'https://host/x.warc.gz'
