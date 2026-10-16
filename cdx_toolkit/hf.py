'''
Hugging Face Storage Buckets (hf://buckets/...): the version of huggingface_hub they need.
'''
import re
from typing import Tuple

# HfFileSystem lists and opens bucket paths since 1.6.0; before 1.7.2, listing a missing path raised
# EntryNotFoundError instead of FileNotFoundError (which the checks of existing outputs rely on)
HF_HUB_MIN_VERSION = (1, 7, 2)


def version_tuple(version: str) -> Tuple[int, ...]:
    """'1.7.2' -> (1, 7, 2), '2.0.0rc1' -> (2, 0, 0)"""
    return tuple(int(re.match(r'\d*', part).group() or 0) for part in version.split('.')[:3])


def check_huggingface_hub() -> None:
    """Raise RuntimeError with a clear message if huggingface_hub is missing or too old for hf://buckets/ paths
    (an installed older version also satisfies a requirement without a minimum)."""
    minimum = '.'.join(map(str, HF_HUB_MIN_VERSION))
    install = f"pip install 'cdx_toolkit[hf]' (or 'huggingface_hub>={minimum}')"
    try:
        import huggingface_hub
    except ImportError:
        raise RuntimeError(f'hf://buckets/ paths need huggingface_hub: {install}') from None
    version = huggingface_hub.__version__
    if version_tuple(version) < HF_HUB_MIN_VERSION:
        raise RuntimeError(f'hf://buckets/ paths need huggingface_hub >= {minimum}, not {version}: {install}')


def uses_hf(args) -> bool:
    """Whether command line arguments name a Hugging Face bucket."""
    return any('hf://' in arg or arg == 'cc-url-index-hf' for arg in args)
