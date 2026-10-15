import requests
import responses
from unittest.mock import patch
import pytest
from cdx_toolkit.myrequests import myrequests_get


@responses.activate
def test_get_without_error():
    responses.add(responses.GET, 'http://example.com/', body='<html></html>')
    resp = myrequests_get('http://example.com')

    assert resp.status_code == 200


def test_get_with_connection_error(caplog, monkeypatch):
    from cdx_toolkit import myrequests

    monkeypatch.setitem(myrequests.RATE_LIMITER.intervals, 'example.com', 0)  # every attempt takes its turn
    with patch('requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.ConnectionError('Connection failed')

        with pytest.raises(ValueError):
            myrequests_get(
                'http://example.com/',  # url does not matter
                retry_max_sec=0.01,
                raise_error_after_n_errors=4,
                raise_warning_after_n_errors=2,
            )

    log_levels = [r.levelname for r in caplog.records]
    assert log_levels.count('WARNING') == 2
    assert log_levels.count('ERROR') == 1


def test_concurrent_requests_keep_the_minimum_interval():
    import threading

    from cdx_toolkit.myrequests import HostRateLimiter

    host = 'interval-test.example.org'
    limiter = HostRateLimiter(3.0, {host: 0.5})
    times = []
    threads = [threading.Thread(target=lambda: times.append(limiter.reserve(host, 1000.0))) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(times) == [1000.0, 1000.5, 1001.0, 1001.5]
    assert limiter.minimum_interval('other.example.org') == 3.0


def test_finish_spaces_the_next_request_from_the_end_of_the_previous_one():
    from cdx_toolkit.myrequests import HostRateLimiter

    limiter = HostRateLimiter(10.0, {})
    with patch('cdx_toolkit.myrequests.time.monotonic', return_value=2000.0):
        limiter.finish('example.org')
    assert limiter.reserve('example.org', 2001.0) == 2010.0


@pytest.mark.parametrize('host, fatal', [('unknown-host.example.org', True), ('data.commoncrawl.org', False)])
def test_dns_error_is_only_fatal_for_unknown_hosts(monkeypatch, host, fatal):
    from cdx_toolkit import myrequests

    monkeypatch.setitem(myrequests.RATE_LIMITER.intervals, host, 0)
    error = requests.exceptions.ConnectionError('[Errno 8] nodename nor servname provided, or not known')
    with patch('requests.get', side_effect=error) as get, pytest.raises(ValueError) as e:
        myrequests_get(f'https://{host}/x', retry_max_sec=0.01, raise_error_after_n_errors=1)
    if fatal:
        assert 'invalid hostname' in str(e.value) and get.call_count == 1
    else:
        assert '2 failures' in str(e.value) and get.call_count == 2  # retried


def test_retries_take_their_turn(monkeypatch):
    """Three parallel requests to a host, the first answered with a 503: its retry keeps the minimum interval
    to the other requests too (it used to start together with the third one)."""
    import threading
    import time
    from unittest.mock import Mock

    from cdx_toolkit import myrequests

    host = 'retry-turn.example.org'
    interval = 0.1
    monkeypatch.setitem(myrequests.RATE_LIMITER.intervals, host, interval)
    starts = []
    lock = threading.Lock()

    def get(url, **kwargs):
        with lock:
            starts.append(time.monotonic())
            first = len(starts) == 1
        return Mock(status_code=503 if first else 200, text='')

    with patch('requests.get', side_effect=get):
        threads = [threading.Thread(target=myrequests_get, args=(f'https://{host}/{i}',)) for i in range(3)]
        for thread in threads:
            thread.start()
            time.sleep(0.01)  # the first thread takes the first turn
        for thread in threads:
            thread.join()

    assert len(starts) == 4
    starts.sort()
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert min(gaps) > interval * 0.8, gaps


ABORTED = "Error: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"


@pytest.mark.parametrize('status', [200, 400, 500])
@responses.activate
def test_cdx_server_connection_error_is_retried(monkeypatch, status):
    """The index server could not read its index: its answer is the exception's text, with any status."""
    from cdx_toolkit import myrequests

    monkeypatch.setitem(myrequests.RATE_LIMITER.intervals, 'cdx.example.org', 0)
    responses.add(responses.GET, 'https://cdx.example.org/cdx', body=ABORTED, status=status)
    responses.add(responses.GET, 'https://cdx.example.org/cdx', body='{"url": "https://example.com/"}\n')

    resp = myrequests_get('https://cdx.example.org/cdx', cdx=True)

    assert resp.status_code == 200 and resp.text.startswith('{')
    assert len(responses.calls) == 2


@responses.activate
def test_cdx_400_without_connection_error_is_not_retried(monkeypatch):
    from cdx_toolkit import myrequests

    monkeypatch.setitem(myrequests.RATE_LIMITER.intervals, 'cdx.example.org', 0)
    responses.add(responses.GET, 'https://cdx.example.org/cdx', body='<html>page too big</html>', status=400)

    assert myrequests_get('https://cdx.example.org/cdx', cdx=True).status_code == 400
    assert len(responses.calls) == 1
