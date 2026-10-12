import cdx_toolkit.warc
from tests.conftest import requires_aws_s3
from unittest.mock import Mock
import pytest


def test_wb_redir_to_original():
    location = 'https://web.archive.org/web/20110209062054id_/http://commoncrawl.org/'
    ret = 'http://commoncrawl.org/'
    assert cdx_toolkit.warc.wb_redir_to_original(location) == ret


@requires_aws_s3
def test_fetch_warc_record_from_s3(s3_env):
    record = cdx_toolkit.warc.fetch_warc_record(
        capture=s3_env.capture({
            'url': 'https://bibliotheque.missiondefrance.fr/index.php?lvl=bulletin_display&id=319',
            'filename': 'crawl-data/CC-MAIN-2024-30/segments/1720763514759.37/warc/CC-MAIN-20240716142214-20240716172214-00337.warc.gz',  # noqa: E501
            'offset': 111440525,
            'length': 9754,
        }),
        warc_download_prefix='s3://commoncrawl',
    )
    record_content = record.content_stream().read().decode(errors='ignore')

    assert record.rec_type == 'response'
    assert record.length == 75825
    assert '<title>Catalogue en ligne Mission de France</title>' in record_content


def test_get_fake_wb_warc_status_code(caplog):
    """Test status code handling in fake_wb_warc function."""

    # Test case 1: Status codes match - no warnings
    caplog.clear()
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.reason = 'OK'
    mock_resp.headers = {'Content-Type': 'text/html'}
    mock_resp.content = b'<html>test</html>'

    capture = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200'}

    record = cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp,
        capture=capture,
    )

    assert record is not None
    assert record.rec_type == 'response'
    assert 'revisit record vivified' not in caplog.text
    assert 'redirect capture came back 200' not in caplog.text
    assert 'status code is now' not in caplog.text

    # Test case 2: Revisit record vivified (200 response, '-' status in capture)
    caplog.clear()
    capture_revisit = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '-'}

    record = cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp,
        capture=capture_revisit,
    )

    assert record is not None
    assert 'revisit record vivified by wayback' in caplog.text

    # Test case 3: Redirect capture came back 200 (200 response, 3xx status in capture)
    caplog.clear()
    capture_redirect = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '301'}

    record = cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp,
        capture=capture_redirect,
    )

    assert record is not None
    assert 'redirect capture came back 200' in caplog.text

    # Test case 4: Wayback returns 302, capture has 3xx status - should use capture status
    caplog.clear()
    mock_resp_302 = Mock()
    mock_resp_302.status_code = 302
    mock_resp_302.reason = 'Found'
    mock_resp_302.headers = {
        'Content-Type': 'text/html',
        'Location': 'https://web.archive.org/web/20240101120000id_/http://example.com/new',
    }
    mock_resp_302.content = b''

    capture_301 = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '301'}

    record = cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp_302,
        capture=capture_301,
    )

    assert record is not None
    # The status line should use 301 from capture, not 302 from response
    assert '301' in record.http_headers.get_statuscode()
    # No warnings for this case - it's expected behavior
    assert 'revisit record vivified' not in caplog.text
    assert 'redirect capture came back 200' not in caplog.text
    assert 'status code is now' not in caplog.text

    # Test case 5: 302 response with 307 capture status - should use 307
    caplog.clear()
    capture_307 = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '307'}

    record = cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp_302,
        capture=capture_307,
    )

    assert record is not None
    assert '307' in record.http_headers.get_statuscode()
    # No warnings for this case either
    assert 'revisit record vivified' not in caplog.text
    assert 'redirect capture came back 200' not in caplog.text
    assert 'status code is now' not in caplog.text

    # Test case 6: Mismatched status codes (not covered by special cases)
    caplog.clear()
    mock_resp_404 = Mock()
    mock_resp_404.status_code = 404
    mock_resp_404.reason = 'Not Found'
    mock_resp_404.headers = {'Content-Type': 'text/html'}
    mock_resp_404.content = b'Not found'

    capture_200 = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200'}

    record = cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp_404,
        capture=capture_200,
    )

    assert record is not None
    # This should trigger the "surprised" warning (else case)
    assert 'status code is now' in caplog.text


def test_unique_warc_filename():
    """Test _unique_warc_filename method of CDXToolkitWARCWriter."""

    # Test case 1: Basic filename generation with gzip and no subprefix
    writer = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='/tmp/test-prefix', subprefix=None, info='test info', gzip=True
    )

    filename = writer._unique_warc_filename()
    # Use the actual file_system_prefix from the writer for cross-platform compatibility
    expected = f'{writer.file_system_prefix}-000000.extracted.warc.gz'
    assert filename == expected
    assert writer.segment == 0

    # Test case 2: Filename generation without gzip
    writer_no_gzip = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='/tmp/test-prefix', subprefix=None, info='test info', gzip=False
    )

    filename = writer_no_gzip._unique_warc_filename()
    expected = f'{writer_no_gzip.file_system_prefix}-000000.extracted.warc'
    assert filename == expected
    assert not filename.endswith('.gz')

    # Test case 3: Filename generation with subprefix
    writer_subprefix = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='/tmp/test-prefix', subprefix='mysub', info='test info', gzip=True
    )

    filename = writer_subprefix._unique_warc_filename()
    expected = f'{writer_subprefix.file_system_prefix}-mysub-000000.extracted.warc.gz'
    assert filename == expected
    assert 'mysub' in filename

    # Test case 4: Filename generation with subprefix and no gzip
    writer_subprefix_no_gzip = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='/tmp/test-prefix', subprefix='another', info='test info', gzip=False
    )

    filename = writer_subprefix_no_gzip._unique_warc_filename()
    expected = f'{writer_subprefix_no_gzip.file_system_prefix}-another-000000.extracted.warc'
    assert filename == expected
    assert 'another' in filename
    assert not filename.endswith('.gz')

    # Test case 5: Handling of existing files - should increment segment
    writer_increment = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='/tmp/test-increment', subprefix=None, info='test info', gzip=True
    )

    # Mock the file_system.exists to simulate existing files
    original_exists = writer_increment.file_system.exists
    call_count = [0]

    def mock_exists(path):
        call_count[0] += 1
        # First two calls return True (files exist), third returns False
        if call_count[0] <= 2:
            return True
        return original_exists(path)

    writer_increment.file_system.exists = mock_exists

    filename = writer_increment._unique_warc_filename()
    # Should have incremented segment twice (0 and 1 existed, 2 is free)
    expected = f'{writer_increment.file_system_prefix}-000002.extracted.warc.gz'
    assert filename == expected
    assert writer_increment.segment == 2

    # Restore original
    writer_increment.file_system.exists = original_exists

    # Test case 6: Multiple segments with subprefix
    writer_multi = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='/tmp/test-multi', subprefix='batch1', info='test info', gzip=True
    )
    writer_multi.segment = 5

    filename = writer_multi._unique_warc_filename()
    expected = f'{writer_multi.file_system_prefix}-batch1-000005.extracted.warc.gz'
    assert filename == expected
    assert '000005' in filename


def test_is_s3_url():
    assert cdx_toolkit.warc._is_s3_url('s3://bucket/key')
    assert cdx_toolkit.warc._is_s3_url('s3:bucket/key')
    assert not cdx_toolkit.warc._is_s3_url('https://example.com/file.warc.gz')


def test_url_to_fs_local_without_fsspec(monkeypatch, tmp_path):
    monkeypatch.setattr(cdx_toolkit.warc, '_HAS_FSSPEC', False)
    monkeypatch.setattr(cdx_toolkit.warc, 'fsspec', None)

    fs, prefix = cdx_toolkit.warc._url_to_fs(str(tmp_path))

    assert isinstance(fs, cdx_toolkit.warc._LocalFileSystem)
    assert prefix == str(tmp_path)

    test_file = tmp_path / 'local.warc'
    with fs.open(str(test_file), 'wb') as handle:
        handle.write(b'test')
    assert fs.exists(str(test_file))


def test_url_to_fs_requires_deps_for_s3(monkeypatch):
    monkeypatch.setattr(cdx_toolkit.warc, '_HAS_FSSPEC', False)
    monkeypatch.setattr(cdx_toolkit.warc, 'fsspec', None)

    with pytest.raises(RuntimeError, match=r'cdx_toolkit\[s3\]'):
        cdx_toolkit.warc._url_to_fs('s3://bucket/key')


def test_fetch_warc_record_requires_s3_deps(monkeypatch):
    monkeypatch.setattr(cdx_toolkit.warc, '_HAS_FSSPEC', False)
    monkeypatch.setattr(cdx_toolkit.warc, 'fsspec', None)

    capture = {
        'url': 'http://example.com',
        'filename': 'dummy.warc.gz',
        'offset': 0,
        'length': 1,
    }

    with pytest.raises(RuntimeError, match=r'cdx_toolkit\[s3\]'):
        cdx_toolkit.warc.fetch_warc_record(capture, warc_download_prefix='s3://bucket')


def _make_fake_record():
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.reason = 'OK'
    mock_resp.headers = {'Content-Type': 'text/html'}
    mock_resp.content = b'<html>test</html>'
    capture = {'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200'}
    return cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=mock_resp,
        capture=capture,
    )


def test_warc_writer_context_manager_basic(tmp_path):
    """The writer must work as a context manager and flush its file on exit."""
    prefix = str(tmp_path / 'ctx')
    with cdx_toolkit.warc.get_writer(prefix, None, {'software': 'test'}) as writer:
        writer.write_record(_make_fake_record())
        filename = writer.filename

    assert writer._file_context is None
    written = list(tmp_path.glob('ctx-*.warc.gz'))
    assert len(written) == 1
    assert written[0].stat().st_size > 0
    assert written[0].name == filename.rsplit('/', 1)[-1]


def test_warc_writer_context_manager_returns_self(tmp_path):
    writer = cdx_toolkit.warc.get_writer(str(tmp_path / 'ret'), None, {'software': 'test'})
    with writer as entered:
        assert entered is writer


def test_warc_writer_context_manager_propagates_exceptions(tmp_path):
    """Exceptions inside the with block must propagate, and cleanup must still run."""
    writer = cdx_toolkit.warc.get_writer(str(tmp_path / 'boom'), None, {'software': 'test'})
    with pytest.raises(RuntimeError, match='boom'):
        with writer:
            writer.write_record(_make_fake_record())
            raise RuntimeError('boom')
    assert writer._file_context is None
    assert writer.file_handler is None


def test_warc_writer_close_idempotent(tmp_path):
    """Calling close() twice (e.g. explicit close then __exit__) must not raise."""
    writer = cdx_toolkit.warc.get_writer(str(tmp_path / 'idem'), None, {'software': 'test'})
    with writer:
        writer.write_record(_make_fake_record())
        writer.close()
    writer.close()
