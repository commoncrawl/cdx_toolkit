import requests
from unittest.mock import patch
import pytest
from cdx_toolkit.myrequests import myrequests_get


def test_get_without_error():
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

    from cdx_toolkit import myrequests

    host = 'interval-test.example.org'
    myrequests.retry_info[host] = {'next_fetch': 1.0, 'minimum_interval': 0.5}
    times = []
    threads = [threading.Thread(target=lambda: times.append(myrequests.reserve_fetch_time(host, 1000.0)))
               for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(times) == [1000.0, 1000.5, 1001.0, 1001.5]
