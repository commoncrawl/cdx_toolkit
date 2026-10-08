from argparse import ArgumentParser
import sys

from cdx_toolkit.indexes import UsageError
from cdx_toolkit.utils import get_version

from . import index, repackage
from .common import add_verbose_arg, set_loglevel
from .deprecated import DEPRECATED_COMMANDS, REMOVED_COMMANDS, exit_removed_command, get_command
from .deprecated import rewrite_deprecated_command


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog='cdxt', description='cdx_toolkit command line tool')
    parser.add_argument('--version', '-V', action='version', version=get_version())
    add_verbose_arg(parser)

    subparsers = parser.add_subparsers(dest='cmd', metavar='COMMAND')
    subparsers.required = True
    index.add_parser(subparsers)
    repackage.add_parser(subparsers)
    return parser


def main(args=None):
    if args is None:  # pragma: no cover
        args = sys.argv[1:]

    command = get_command(args)
    if command in REMOVED_COMMANDS:
        exit_removed_command(command, args)
    if command in DEPRECATED_COMMANDS:
        args = rewrite_deprecated_command(args)

    parser = build_parser()
    cmd = parser.parse_args(args=args)
    set_loglevel(cmd.verbose)
    cmdline = ' '.join(['cdxt'] + list(args))
    try:
        cmd.func(cmd, cmdline)
    except (UsageError, FileNotFoundError) as e:
        parser.exit(2, f'cdxt {cmd.cmd}: error: {e}\n')
