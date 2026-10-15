"""A failing worker process stops the others (real spawned processes)."""
import multiprocessing
import os
import time

import pytest

from cdx_toolkit.repackage import multiprocess
from cdx_toolkit.repackage.multiprocess import run_worker_processes


def worker(config):
    """'fail': fails after a moment; 'wait': waits for the cancel event, then stops (or completes after 20 s);
    'ignore': does not look at the cancel event."""
    kind, marker = config
    if kind == 'fail':
        time.sleep(0.2)
        raise OSError('disk full')
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if kind == 'wait' and multiprocess._CANCEL.is_set():
            return 'stopped'
        time.sleep(0.05)
    with open(marker, 'w') as f:
        f.write('completed')
    return 'completed'


@pytest.mark.parametrize('failing_first', [True, False])
def test_a_failing_worker_stops_the_others(tmp_path, failing_first):
    configs = [('wait', str(tmp_path / 'a')), ('wait', str(tmp_path / 'b'))]
    configs.insert(0 if failing_first else len(configs), ('fail', None))
    ctx = multiprocessing.get_context('spawn')
    start = time.monotonic()
    with pytest.raises(OSError, match='disk full'):
        run_worker_processes(worker, configs, ctx.Value('i', 0), ctx, cancel_timeout=15)
    assert time.monotonic() - start < 15  # the others did not run to completion (20 s)
    assert os.listdir(tmp_path) == []


def test_a_worker_that_does_not_stop_is_terminated(tmp_path):
    configs = [('fail', None), ('ignore', str(tmp_path / 'a'))]
    ctx = multiprocessing.get_context('spawn')
    start = time.monotonic()
    with pytest.raises(OSError, match='disk full'):
        run_worker_processes(worker, configs, ctx.Value('i', 0), ctx, cancel_timeout=1)
    assert time.monotonic() - start < 15
    time.sleep(0.5)
    assert os.listdir(tmp_path) == []


def test_results_in_the_order_of_the_configs(tmp_path):
    ctx = multiprocessing.get_context('spawn')
    file_sequence = ctx.Value('i', 0)
    assert run_worker_processes(abs, [-3, 1, -2], file_sequence, ctx) == [3, 1, 2]
