from argparse import SUPPRESS, ArgumentParser, ArgumentTypeError
import logging
import os
import re


LOGGER = logging.getLogger(__name__)


class IncompleteError(RuntimeError):
    """The command ran, but did not do all of its work, e.g. captures could not be fetched (exit status 1)."""


SIZE_UNITS = {
    '': 1,
    'b': 1,
    'k': 1000, 'kb': 1000, 'kib': 1024,
    'm': 1000 ** 2, 'mb': 1000 ** 2, 'mib': 1024 ** 2,
    'g': 1000 ** 3, 'gb': 1000 ** 3, 'gib': 1024 ** 3,
    't': 1000 ** 4, 'tb': 1000 ** 4, 'tib': 1024 ** 4,
}


def add_verbose_arg(parser: ArgumentParser, subcommand: bool = False):
    # On subcommands the default is suppressed, so it does not override a -v given before the subcommand.
    parser.add_argument(
        '--verbose',
        '-v',
        action='count',
        default=SUPPRESS if subcommand else 0,
        help='set logging level to INFO (-v) or DEBUG (-vv)',
    )


def set_loglevel(verbose: int):
    loglevel = os.getenv('CDXT_LOGLEVEL') or os.getenv('LOGLEVEL') or 'WARNING'  # LOGLEVEL: the old name
    if verbose and verbose > 0:
        loglevel = 'INFO'
    if verbose and verbose > 1:
        loglevel = 'DEBUG'

    # because pytest has already initialized the logger, we have to set the root logger
    logging.getLogger().setLevel(loglevel)
    # for the normal case
    logging.basicConfig(level=loglevel)

    LOGGER.info('set loglevel to %s', str(loglevel))


def parse_size(value: str) -> int:
    """Parse a size in bytes like 1000000, 100MB, 1.5GB or 1GiB (argparse type)."""
    match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*', value)
    if not match or match.group(2).lower() not in SIZE_UNITS:
        raise ArgumentTypeError(f'invalid size {value!r}, use e.g. 1000000, 100MB or 1GiB')
    return int(float(match.group(1)) * SIZE_UNITS[match.group(2).lower()])


def parse_fields(value: str):
    """Comma-separated list of field names (argparse type)."""
    return [f.strip() for f in value.split(',') if f.strip()]
