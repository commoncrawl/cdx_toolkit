from io import BytesIO
import fsspec
import pytest
import cdx_toolkit

from tests.conftest import TEST_DATA_PATH, requires_aws_s3

from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator


fixture_path = TEST_DATA_PATH / 'repackage'


@pytest.mark.parametrize(
    'prefix,gzip',
    [
        pytest.param('test-prefix', False, id='File name prefix on local'),
        pytest.param('test-prefix', True, id='File name prefix on local with gzip'),
        # raised FileNotFound error (parent dir does not exist)
        # pytest.param("test-prefix-folder/file-prefix", None, id="Folder as prefix"),
    ],
)
def test_write_to_local(prefix, gzip, tmpdir):
    info = {
        'software': 'pypi_cdx_toolkit/test',
        'description': 'test',
        'format': 'WARC file version 1.0',
    }
    encoding = 'utf-8'
    full_prefix = str(tmpdir) + '/' + prefix
    fs, fs_prefix_path = fsspec.url_to_fs(full_prefix)

    with pytest.warns(FutureWarning, match='deprecated'):
        writer = cdx_toolkit.warc.get_writer(full_prefix, None, info, gzip=gzip)

    # single record
    input_resource_record_text = 'foo bar text'
    writer.write_record(
        WARCWriter(None).create_warc_record(
            uri='foo/bar',
            record_type='resource',
            payload=BytesIO(input_resource_record_text.encode(encoding)),
            warc_content_type='text/plain',
        )
    )
    writer.close()

    # Check that WARC file was created
    warc_path = fs_prefix_path + '-000000.extracted.warc'
    if gzip:
        warc_path += '.gz'

    assert fs.exists(warc_path)

    # Validate that creator/operator are not in warcinfo record
    info_record = None
    resource_record = None
    with open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode(encoding)

            if record.rec_type == 'resource':
                resource_record = record.content_stream().read().decode(encoding)
                break

    assert resource_record is not None
    assert info_record is not None

    assert 'description: test' in info_record
    assert resource_record == input_resource_record_text


@requires_aws_s3
def test_write_to_s3(s3_tmpdir):
    info = {
        'software': 'pypi_cdx_toolkit/test',
        'description': 'test',
        'format': 'WARC file version 1.0',
    }
    encoding = 'utf-8'

    fs, fs_prefix_path = fsspec.url_to_fs(s3_tmpdir)

    with pytest.warns(FutureWarning, match='deprecated'):
        writer = cdx_toolkit.warc.get_writer(s3_tmpdir, None, info)

    # single record
    input_resource_record_text = 'foo bar text'
    writer.write_record(
        WARCWriter(None).create_warc_record(
            uri='foo/bar',
            record_type='resource',
            payload=BytesIO(input_resource_record_text.encode(encoding)),
            warc_content_type='text/plain',
        )
    )
    writer.close()

    # Check that WARC file was created
    warc_path = fs_prefix_path + '-000000.extracted.warc.gz'
    assert fs.exists(warc_path)

    # Validate that creator/operator are not in warcinfo record
    info_record = None
    resource_record = None
    with fs.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode(encoding)

            if record.rec_type == 'resource':
                resource_record = record.content_stream().read().decode(encoding)
                break

    assert resource_record is not None
    assert info_record is not None

    assert 'description: test' in info_record
    assert resource_record == input_resource_record_text


def test_warc_info():
    warc_version = '1.0'
    gzip = False
    file_handler = BytesIO()
    filename = 'foo.warc'

    info = {
        'software': 'pypi_cdx_toolkit/123',
        'isPartOf': 'bar',
        'description': 'warc extraction based on CDX generated with: xx',
        'format': 'WARC file version 1.0',
    }

    writer = WARCWriter(file_handler, gzip=gzip, warc_version=warc_version)
    warcinfo = writer.create_warcinfo_record(filename, info)

    writer.write_record(warcinfo)

    file_value = file_handler.getvalue().decode('utf-8')

    assert 'pypi_cdx_toolkit/123' in file_value
