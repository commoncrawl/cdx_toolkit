from tests.conftest import requires_aws_s3
from cdx_toolkit.warc import fetch_warc_record


def test_fetch_warc_record_from_http():
    encoding = 'utf-8'
    capture = {
        'url': 'https://bibliotheque.missiondefrance.fr/index.php?lvl=bulletin_display&id=319',
        'mime': 'text/html',
        'mime-detected': 'application/xhtml+xml',
        'status': '200',
        'digest': 'D5K3FUWDRAOMMTJC2CTWV7L2ABFIJ5BP',
        'length': '9754',
        'offset': '111440525',
        'filename': 'crawl-data/CC-MAIN-2024-30/segments/1720763514759.37/warc/CC-MAIN-20240716142214-20240716172214-00337.warc.gz',  # noqa: E501
        'charset': 'UTF-8',
        'languages': 'fra',
        'timestamp': '20240716153155',
    }
    warc_download_prefix = 'https://data.commoncrawl.org'

    record = fetch_warc_record(capture, warc_download_prefix)
    record_content = record.content_stream().read().decode(encoding, errors='ignore')

    assert record.rec_type == 'response'
    assert record.length == 75825
    assert '<title>Catalogue en ligne Mission de France</title>' in record_content


@requires_aws_s3
def test_fetch_warc_record_from_s3():
    encoding = 'utf-8'
    capture = {
        'url': 'https://bibliotheque.missiondefrance.fr/index.php?lvl=bulletin_display&id=319',
        'mime': 'text/html',
        'mime-detected': 'application/xhtml+xml',
        'status': '200',
        'digest': 'D5K3FUWDRAOMMTJC2CTWV7L2ABFIJ5BP',
        'length': '9754',
        'offset': '111440525',
        'filename': 'crawl-data/CC-MAIN-2024-30/segments/1720763514759.37/warc/CC-MAIN-20240716142214-20240716172214-00337.warc.gz',  # noqa: E501
        'charset': 'UTF-8',
        'languages': 'fra',
        'timestamp': '20240716153155',
    }
    warc_download_prefix = 's3://commoncrawl'

    record = fetch_warc_record(capture, warc_download_prefix)
    record_content = record.content_stream().read().decode(encoding, errors='ignore')

    assert record.rec_type == 'response'
    assert record.length == 75825
    assert '<title>Catalogue en ligne Mission de France</title>' in record_content
