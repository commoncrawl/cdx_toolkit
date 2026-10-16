import logging
import json
from collections.abc import MutableMapping
import warnings

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:   # pragma: no cover
    # Python < 3.8 compatibility
    from importlib_metadata import version, PackageNotFoundError  # pragma: no cover

__version__ = 'installed-from-git'

from .myrequests import myrequests_get
from .compat import munge_fields, munge_filter
from .commoncrawl import get_cc_endpoints, apply_cc_defaults, filter_cc_endpoints
from .warc import fetch_wb_warc, fetch_warc_record
from .timeutils import validate_timestamps

LOGGER = logging.getLogger(__name__)

try:
    # this works for the pip-installed package
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    pass


lines_per_page = 3000  # no way to get this from the API without fetching a page


def showNumPages(r):
    j = r.json()
    if isinstance(j, dict):  # pywb always returns json
        pages = int(j.get('blocks', 0))
    elif isinstance(j, int):  # ia always returns text, parsed as a json int
        pages = j
    else:
        raise ValueError('surprised by showNumPages value of '+str(j))
    return pages


def pages_to_samples(pages):
    # adjust pages for the partial page at the start and end
    if pages > 1:
        pages = pages - 1.0
    elif pages >= 1:
        pages = pages - 0.5
    pages *= lines_per_page
    return int(pages)


def cdx_to_captures(resp, wb=None, warc_download_prefix=None):
    if resp.status_code == 404:
        # this is an empty result for pywb iff {"error": "No Captures found for: ..."}
        if resp.text.startswith('{'):
            j = json.loads(resp.text)
            if 'error' in j or 'message' in j:
                return []
        LOGGER.debug('404 seen for API call, body is %s', resp.text)
        raise ValueError('404 seen for API call, did you configure the endpoint correctly?')

    text = resp.text

    # pywb output='json' is jsonl
    if text.startswith('{'):
        lines = resp.text.splitlines()
        ret = []
        for line in lines:
            ret.append(CaptureObject(json.loads(line), wb=wb, warc_download_prefix=warc_download_prefix))
        return ret

    # ia output='json' is a json list of lists
    if text.startswith('['):
        if text.startswith('[]'):
            return []

        try:
            lines = json.loads(text)
            fields = lines.pop(0)
        except (json.decoder.JSONDecodeError, KeyError, IndexError):  # pragma: no cover
            raise ValueError('cannot decode response, first bytes are '+repr(text[:50]))

        ret = munge_fields(fields, lines)
        return [CaptureObject(r, wb=wb, warc_download_prefix=warc_download_prefix) for r in ret]

    raise ValueError(f'cannot decode response (status {resp.status_code}), first bytes are {text[:50]!r}')


class CaptureObject(MutableMapping):
    '''
    Represents a single capture of a webpage, plus less-visible info about how to fetch the content.
    '''
    def __init__(self, data, wb=None, warc_download_prefix=None):
        self.data = data
        self.wb = wb
        self.warc_download_prefix = warc_download_prefix
        self.warc_record = None
        self._content = None

    def is_revisit(self):
        if self.wb and 'mime' in self.data and self.data['mime'] == 'warc/revisit':
            # also: status == '-'
            return True
        return False

    def fetch_warc_record(self):
        if self.warc_record is not None:
            return self.warc_record
        if self.wb:
            self.warc_record = fetch_wb_warc(self.data, wb=self.wb)
        elif self.warc_download_prefix:
            self.warc_record = fetch_warc_record(self.data, self.warc_download_prefix)
        else:
            raise ValueError('no content source configured')
        return self.warc_record

    @property
    def content_stream(self):
        return self.fetch_warc_record().content_stream()

    @property
    def content(self):
        if self._content:
            return self._content
        self._content = self.fetch_warc_record().content_stream().read()
        return self._content

    @property
    def text(self):
        '''
        Eventually this function will do something with the character set, but not yet.
        '''
        return self.content.decode('utf-8', errors='replace')

    # the remaining code treats self.data like a dict

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __contains__(self, key):
        return key in self.data

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)


class CDXFetcherIter:
    def __init__(self, cdxfetcher, params={}, index_list=None):
        self.cdxfetcher = cdxfetcher
        self.params = params
        if 'page' in params:  # pragma: no cover
            raise ValueError('must not set page= in a CDX iterator')
        self.endpoint = 0
        self.page = -1
        self.params['page'] = self.page
        self.captures = []
        self.index_list = index_list

        self.get_more()

    def get_more(self):
        while True:
            self.page += 1

            if self.page == 0 and len(self.index_list) > 0 and self.endpoint < len(self.index_list):
                LOGGER.info('get_more: fetching cdx from %s', self.index_list[self.endpoint])

            status, objs = self.cdxfetcher.get_for_iter(self.endpoint, self.page,
                                                        params=self.params, index_list=self.index_list)
            if status == 'last endpoint':
                LOGGER.debug('get_more: I have reached the end')
                return  # caller will raise StopIteration
            if status == 'last page':
                LOGGER.debug('get_more: moving to next endpoint')
                self.endpoint += 1
                self.page = -1
                continue
            LOGGER.debug('get_more, got %d more objs', len(objs))
            self.captures.extend(objs)
            if len(self.captures) > 0:
                break

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                return self.captures.pop(0)
            except IndexError:
                LOGGER.debug('getting more in __next__')
                self.get_more()
                if len(self.captures) <= 0:
                    # XXX print out a warning if this hits the default limit of 1000
                    raise StopIteration


class CDXFetcher:
    def __init__(self,
                 source='cc',
                 crawl=None,
                 wb=None,
                 warc_download_prefix=None,
                 cc_mirror=None,
                 cc_sort='mixed',
                 loglevel=None):
        self.source = source
        self.crawl = crawl
        self.cc_sort = cc_sort
        self.source = source
        if wb is not None and warc_download_prefix is not None:
            raise ValueError('cannot specify both wb and warc_download_prefix')
        self.wb = wb
        self.warc_download_prefix = warc_download_prefix

        if source == 'cc':
            if wb is not None:
                raise ValueError('cannot specify wb= for source=cc')
            self.cc_mirror = cc_mirror or 'https://index.commoncrawl.org/'
            self.raw_index_list = get_cc_endpoints(self.cc_mirror)
            self.warc_download_prefix = warc_download_prefix or 'https://data.commoncrawl.org'
        elif source == 'ia':
            self.index_list = ('https://web.archive.org/cdx/search/cdx',)
            if self.warc_download_prefix is None and self.wb is None:
                self.wb = 'https://web.archive.org/web'
        elif source.startswith('https://') or source.startswith('http://'):
            self.index_list = (source,)
        else:
            raise ValueError('could not understand source')

        if loglevel:
            LOGGER.setLevel(level=loglevel)

    def customize_index_list(self, params):
        if self.source == "cc" and (
            self.crawl
            or "crawl" in params
            or "from" in params
            or "from_ts" in params
            or "to" in params
            or "closest" in params
        ):
            LOGGER.info("making a custom cc index list")
            if self.crawl and "crawl" not in params:
                params["crawl"] = self.crawl
            return filter_cc_endpoints(self.raw_index_list, self.cc_sort, params=params)

        return self.index_list

    def get(self, url, **kwargs):
        # from_ts=None, to=None, matchType=None, limit=None, sort=None, closest=None,
        # filter=None, fl=None, page=None, pageSize=None, showNumPages=None):
        params = kwargs
        validate_timestamps(params)
        params['url'] = url
        params['output'] = 'json'
        if 'crawl' not in params:
            params['crawl'] = self.crawl
        if 'filter' in params:
            if isinstance(params['filter'], str):
                params['filter'] = (params['filter'],)
            params['filter'] = munge_filter(params['filter'], self.source)

        if 'limit' not in params:
            LOGGER.info('adding default limit=1000 to get')
            params['limit'] = 1000
        if self.source == 'cc':
            apply_cc_defaults(params)

        index_list = self.customize_index_list(params)

        ret = []
        for endpoint in index_list:
            resp = myrequests_get(endpoint, params=params, cdx=True)
            objs = cdx_to_captures(
                resp,
                wb=self.wb,
                warc_download_prefix=self.warc_download_prefix
                )  # turns 400 and 404 into []
            ret.extend(objs)
            if 'limit' in params:
                params['limit'] -= len(objs)
                if params['limit'] <= 0:
                    break
        return ret

    def iter(self, url, **kwargs):
        params = kwargs
        validate_timestamps(params)
        params['url'] = url
        params['output'] = 'json'
        if 'crawl' not in params:
            params['crawl'] = self.crawl
        if 'filter' in params:
            if isinstance(params['filter'], str):
                params['filter'] = (params['filter'],)
            params['filter'] = munge_filter(params['filter'], self.source)

        if self.source == 'cc':
            apply_cc_defaults(params, crawl_present=bool(self.crawl))

        index_list = self.customize_index_list(params)
        return CDXFetcherIter(self, params=params, index_list=index_list)

    def items(self, url, **kwargs):  # pragma: no cover
        warnings.warn(
            'cdx.items() has been renamed to cdx.iter() and will be removed in cdx_toolkit 1.0',
            FutureWarning
        )
        return self.iter(url, **kwargs)

    def get_for_iter(self, endpoint, page, params={}, index_list=None):
        '''
        Specalized get for the iterator
        '''
        if endpoint >= len(index_list):
            return 'last endpoint', []
        if params.get('limit', -1) == 0:
            return 'last endpoint', []  # a little white lie

        endpoint = index_list[endpoint]
        params['page'] = page
        resp = myrequests_get(endpoint, params=params, cdx=True)
        if resp.status_code == 400:  # pywb
            return 'last page', []
        if resp.text == '':  # ia
            return 'last page', []

        ret = cdx_to_captures(resp, wb=self.wb, warc_download_prefix=self.warc_download_prefix)  # turns 404 into []
        if 'limit' in params:
            params['limit'] -= len(ret)
        return 'ok', ret

    def get_size_estimate(self, url, as_pages=False, **kwargs):
        '''
        Get the number of pages that match url

        useful additional args: matchType='host' pageSize=1
        or, url can end with * or start with *. to set the matchType
        '''
        if 'details' in kwargs:
            details = True
            del kwargs['details']
        else:
            details = False

        params = {'url': url, 'showNumPages': 'true'}
        params.update(**kwargs)
        validate_timestamps(params)
        if self.source == 'cc':
            apply_cc_defaults(params)

        index_list = self.customize_index_list(params)

        total_pages = 0
        total_samples = 0
        for endpoint in index_list:
            resp = myrequests_get(endpoint, params=params, cdx=True)
            if resp.status_code == 200:
                pages = showNumPages(resp)
                total_pages += pages
                samples = pages_to_samples(pages)
                total_samples += samples
                if details:
                    print(endpoint, samples)
                if 'limit' in kwargs and samples > kwargs['limit']:
                    break
            else:
                pass  # silently ignore empty answers  # pragma: no cover

        if as_pages:
            return total_pages
        else:
            return total_samples
