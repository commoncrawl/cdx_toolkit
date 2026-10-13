from urllib.parse import quote
from io import BytesIO
import datetime
import logging
import os

try:
    import fsspec
    _HAS_FSSPEC = True
except ImportError:  # pragma: no cover - exercised in minimal installs
    fsspec = None
    _HAS_FSSPEC = False
from warcio import WARCWriter
from warcio.recordloader import ArcWarcRecordLoader
from warcio.bufferedreaders import DecompressingBufferedReader
from warcio.statusandheaders import StatusAndHeaders

from .myrequests import myrequests_get
from .timeutils import http_date_to_datetime, datetime_to_iso_date

LOGGER = logging.getLogger(__name__)


def _is_s3_url(url):
    return url.startswith('s3://') or url.startswith('s3:')


def _require_s3_deps():
    if not _HAS_FSSPEC:
        raise RuntimeError(
            'Remote filesystem (S3) support requires optional dependencies. Install cdx_toolkit[s3].'
        )


class _LocalFileSystem:
    def open(self, filename, mode):
        return open(filename, mode)

    def exists(self, filename):
        return os.path.exists(filename)


def _url_to_fs(prefix):
    if _HAS_FSSPEC:
        return fsspec.url_to_fs(prefix)

    if _is_s3_url(prefix) or '://' in prefix:
        _require_s3_deps()

    return _LocalFileSystem(), prefix


def wb_redir_to_original(location):
    return 'http' + location.split('_/http', 1)[1]


http_status_text = {
    300: 'Multiple Choices',
    301: 'Moved Permanently',
    302: 'Found',
    303: 'See Other',
    304: 'Not Modified',
    307: 'Temporary Redirect',
    308: 'Permanent Redirect',
}


def fake_wb_warc(url, wb_url, resp, capture):
    """
    Given a playback from a wayback, fake up a warc response record
    """
    status_code = resp.status_code
    status_reason = resp.reason

    if str(status_code) != capture['status']:
        url = capture['url']
        timestamp = capture['timestamp']
        if status_code == 200 and capture['status'] == '-':
            LOGGER.warning('revisit record vivified by wayback for %s %s', url, timestamp)
        elif status_code == 200 and capture['status'].startswith('3'):
            LOGGER.warning('redirect capture came back 200, same-surt same-timestamp capture? %s %s', url, timestamp)
        elif status_code == 302 and capture['status'].startswith('3'):
            # this is OK, wayback always sends a temporary redir
            status_code = int(capture['status'])
            if status_code != resp.status_code and status_code in http_status_text:
                status_reason = http_status_text[status_code]
        else:  # pragma: no cover
            LOGGER.warning(
                f'status code is now={status_code}, orig={capture["status"]}, {url}, {timestamp}'
            )

    http_headers = []
    http_date = None
    for k, v in resp.headers.items():
        kl = k.lower()
        if kl.startswith('x-archive-orig-date'):
            http_date = v

        if kl.startswith('x-archive-orig-'):
            k = k[len('x-archive-orig-'):]
            http_headers.append((k, v))
        elif kl == 'content-type':
            http_headers.append(('Content-Type', v))
        elif kl == 'location':
            v = wb_redir_to_original(v)
            http_headers.append((k, v))
        else:
            if not kl.startswith('x-archive-'):
                k = 'X-Archive-' + k
            http_headers.append((k, v))

    statusline = f'{status_code} {status_reason}'
    http_headers = StatusAndHeaders(statusline, headers=http_headers, protocol='HTTP/1.1')

    warc_headers_dict = {
        'WARC-Source-URI': wb_url,
        'WARC-Creation-Date': datetime_to_iso_date(datetime.datetime.now()),
    }
    if http_date:
        warc_headers_dict['WARC-Date'] = datetime_to_iso_date(http_date_to_datetime(http_date))

    content_bytes = resp.content

    writer = WARCWriter(None)  # needs warc_version here?
    return writer.create_warc_record(
        url, 'response', payload=BytesIO(content_bytes), http_headers=http_headers, warc_headers_dict=warc_headers_dict
    )


def wb_replay_url(capture, wb, modifier='id_'):
    """The URL of the capture in the wayback machine `wb` (with `modifier` id_: the original content)."""
    return f'{wb}/{capture["timestamp"]}{modifier}/{quote(capture["url"])}'


def fetch_wb_warc(capture, wb, modifier='id_'):
    for field in ('url', 'timestamp', 'status'):
        if field not in capture:  # pragma: no cover
            raise ValueError('capture must contain ' + field)

    if wb is None:  # pragma: no cover
        raise ValueError('No wayback configured')

    url = capture['url']
    wb_url = wb_replay_url(capture, wb, modifier)

    kwargs = {}
    status = capture['status']
    if status == '404' or status == '-':
        # '-' is a revisit; it will 404 if the underlying record is a 404
        # (also has 'mime': 'warc/revisit')
        kwargs['allow404'] = True

    resp = myrequests_get(wb_url, **kwargs)

    return fake_wb_warc(url, wb_url, resp, capture)


def fetch_warc_record(capture, warc_download_prefix):
    for field in ('url', 'filename', 'offset', 'length'):
        if field not in capture:  # pragma: no cover
            raise ValueError('capture must contain ' + field)

    url = capture['url']
    filename = capture['filename']
    offset = int(capture['offset'])
    length = int(capture['length'])

    warc_url = warc_download_prefix + '/' + filename

    if _is_s3_url(warc_url):
        # fetch from S3
        _require_s3_deps()
        with fsspec.open(warc_url, 'rb') as f:
            f.seek(offset)
            record_bytes = f.read(length)
    else:
        # fetch over HTTP
        headers = {'Range': f'bytes={offset}-{offset + length - 1}'}

        resp = myrequests_get(warc_url, headers=headers)
        record_bytes = resp.content

    stream = DecompressingBufferedReader(BytesIO(record_bytes))
    record = ArcWarcRecordLoader().parse_record_stream(stream)

    for header_field in ('WARC-Source-URI', 'WARC-Source-Range'):
        if record.rec_headers.get_header(header_field):  # pragma: no cover
            LOGGER.error(f'Header field {header_field} is already set in this WARC record')

    warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')
    if url != warc_target_uri:  # pragma: no cover
        LOGGER.error(
            f'WARC-Target-URI {warc_target_uri} is not the capture url {url}',
        )

    record.rec_headers.replace_header('WARC-Source-URI', warc_url)
    record.rec_headers.replace_header('WARC-Source-Range', f'bytes={offset}-{offset + length - 1}')
    return record


class CDXToolkitWARCWriter:
    """Writer for WARC files.

    The fsspec package is used for writting to local or remote file system, e.g., S3."""

    def __init__(self, prefix, subprefix, info, size=1000000000, gzip=True, warc_version=None):
        self.prefix = prefix
        self.subprefix = subprefix
        self.info = info
        self.size = size
        self.gzip = gzip
        self.warc_version = warc_version
        self.segment = 0
        self.writer = None
        self.file_handler = None
        self.file_system, self.file_system_prefix = _url_to_fs(self.prefix)
        self._file_context = None

    def write_record(self, *args, **kwargs):
        if self.writer is None:
            if self.warc_version is None:
                # opportunity to intuit warc version here
                self.warc_version = '1.0'
            if self.warc_version != '1.0':
                LOGGER.error('WARC versions other than 1.0 are not correctly supported yet')
                # ...because fake_wb_warc always generates 1.0
            # should we also check the warcinfo record to make sure it's got a matching warc_version inside?
            self._start_new_warc()

        self.writer.write_record(*args, **kwargs)

        # Compare file size of current segment with max. file size
        if self.file_handler and self.file_handler.tell() > self.size:
            self._close_current_file()
            self.writer = None
            self.segment += 1

    def _unique_warc_filename(self):
        while True:
            name = self.file_system_prefix + '-'
            if self.subprefix is not None:
                name += self.subprefix + '-'
            name += f'{self.segment:06d}.extracted.warc'
            if self.gzip:
                name += '.gz'
            if self.file_system.exists(name):
                self.segment += 1
            else:
                break
        return name

    def _start_new_warc(self):
        self.filename = self._unique_warc_filename()
        self._file_context = self.file_system.open(self.filename, 'wb')
        self.file_handler = self._file_context.__enter__()
        LOGGER.info('opening new warc file %s', self.filename)
        self.writer = WARCWriter(self.file_handler, gzip=self.gzip, warc_version=self.warc_version)
        warcinfo = self.writer.create_warcinfo_record(self.filename, self.info)
        self.writer.write_record(warcinfo)

    def _close_current_file(self):
        # Close the handler of the current file (needed for fsspec abstraction)
        if self._file_context is not None:
            self._file_context.__exit__(None, None, None)
            self._file_context = None
            self.file_handler = None

    def close(self):
        # Close the WARC writer (this must be called at the end)
        self._close_current_file()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def get_writer(prefix, subprefix, info, **kwargs):
    return CDXToolkitWARCWriter(prefix, subprefix, info, **kwargs)
