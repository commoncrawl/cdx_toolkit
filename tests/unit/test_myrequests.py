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


def test_get_with_connection_error(caplog):
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
    with patch('cdx_toolkit.myrequests.time.time', return_value=2000.0):
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
