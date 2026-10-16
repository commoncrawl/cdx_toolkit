import sys
from unittest.mock import patch

import pytest

from cdx_toolkit.cli import main
from cdx_toolkit.hf import check_huggingface_hub, uses_hf, version_tuple


def test_version_tuple():
    assert version_tuple('1.7.2') == (1, 7, 2)
    assert version_tuple('2.0.0rc1') == (2, 0, 0)
    assert version_tuple('1.10') == (1, 10)
    assert version_tuple('1.7.2') >= (1, 7, 2) > version_tuple('1.7.1') > version_tuple('0.30.0')


@pytest.mark.parametrize('version, ok', [('0.30.0', False), ('1.7.1', False), ('1.7.2', True), ('2.0.0', True)])
def test_check_huggingface_hub(version, ok):
    with patch.dict(sys.modules, {'huggingface_hub': type(sys)('huggingface_hub')}):
        sys.modules['huggingface_hub'].__version__ = version
        if ok:
            check_huggingface_hub()
        else:
            with pytest.raises(RuntimeError, match=r'huggingface_hub >= 1\.7\.2'):
                check_huggingface_hub()


def test_missing_huggingface_hub():
    with patch.dict(sys.modules, {'huggingface_hub': None}), pytest.raises(RuntimeError, match=r'cdx_toolkit\[hf\]'):
        check_huggingface_hub()


def test_cli_checks_the_version_first(capsys):
    assert uses_hf(['index', 'cc-url-index-hf']) and uses_hf(['repackage', 'x', '--prefix', 'hf://buckets/a/b/c'])
    assert not uses_hf(['index', 'cc', '--url', 'example.com/*'])
    with patch.dict(sys.modules, {'huggingface_hub': type(sys)('huggingface_hub')}), pytest.raises(SystemExit) as e:
        sys.modules['huggingface_hub'].__version__ = '0.30.0'
        main(['index', 'cc-url-index-hf', '--domain', 'example.com', '--dry-run'])
    assert e.value.code == 2
    assert 'huggingface_hub >= 1.7.2, not 0.30.0' in capsys.readouterr().err
