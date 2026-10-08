from unittest.mock import patch

import pytest

from cdx_toolkit.indexes.sql import confirm_cost
from cdx_toolkit.repackage.sources.base import CostEstimate


def est(n_crawls):
    return CostEstimate(n_crawls=n_crawls, engine='athena')


def test_none_estimate_never_prompts():
    # cdx/csv sources return None -> no cost prompt
    with patch('builtins.input') as inp:
        confirm_cost(None, confirmed=False)
        inp.assert_not_called()


def test_small_crawl_set_no_prompt():
    with patch('builtins.input') as inp:
        confirm_cost(est(5), confirmed=False)
        inp.assert_not_called()


def test_confirmed_flag_bypasses_prompt():
    with patch('builtins.input') as inp:
        confirm_cost(est(None), confirmed=True)
        inp.assert_not_called()


def test_large_crawl_set_non_tty_aborts():
    with patch('cdx_toolkit.indexes.sql.sys.stdin') as stdin:
        stdin.isatty.return_value = False
        with pytest.raises(SystemExit):
            confirm_cost(est(11), confirmed=False)


def test_unknown_crawls_non_tty_aborts():
    with patch('cdx_toolkit.indexes.sql.sys.stdin') as stdin:
        stdin.isatty.return_value = False
        with pytest.raises(SystemExit):
            confirm_cost(est(None), confirmed=False)


def test_tty_yes_proceeds():
    with patch('cdx_toolkit.indexes.sql.sys.stdin') as stdin, patch('builtins.input', return_value='y'):
        stdin.isatty.return_value = True
        confirm_cost(est(None), confirmed=False)  # should not raise


def test_tty_empty_answer_aborts():
    with patch('cdx_toolkit.indexes.sql.sys.stdin') as stdin, patch('builtins.input', return_value=''):
        stdin.isatty.return_value = True
        with pytest.raises(SystemExit):
            confirm_cost(est(None), confirmed=False)


def test_tty_no_answer_aborts():
    with patch('cdx_toolkit.indexes.sql.sys.stdin') as stdin, patch('builtins.input', return_value='n'):
        stdin.isatty.return_value = True
        with pytest.raises(SystemExit):
            confirm_cost(est(11), confirmed=False)
