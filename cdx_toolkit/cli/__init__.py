from argparse import ArgumentParser
import logging
import os
import sys
import traceback

from cdx_toolkit.aws import configure_fsspec_s3
from cdx_toolkit.hf import check_huggingface_hub, uses_hf
from cdx_toolkit.indexes import UsageError
from cdx_toolkit.utils import get_version

from . import index, repackage
from .common import IncompleteError, add_verbose_arg, set_loglevel
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
    configure_fsspec_s3()  # Common Crawl's S3 defaults for s3:// paths, unless configured by the user
    cmdline = ' '.join(['cdxt'] + list(args))
    if uses_hf(args):
        try:
            check_huggingface_hub()  # before any work, not when the first hf:// path is opened
        except RuntimeError as e:
            parser.exit(2, f'cdxt {cmd.cmd}: error: {e}\n')
    running = source_threads()  # of other runs in this process (e.g. of the Python API)
    try:
        cmd.func(cmd, cmdline)
    except (UsageError, FileNotFoundError) as e:
        exit_cli(parser, 2, f'cdxt {cmd.cmd}: error: {e}\n', running)
    except IncompleteError as e:
        exit_cli(parser, 1, f'cdxt {cmd.cmd}: error: {e}\n', running)
    except KeyboardInterrupt:
        exit_cli(parser, 130, f'cdxt {cmd.cmd}: interrupted\n', running)
    except Exception:
        if not source_threads() - running:
            raise
        exit_cli(parser, 1, traceback.format_exc(), running)


def source_threads() -> set:
    """The running source threads of cdxt repackage (see cdx_toolkit.repackage.repackager.source_threads)."""
    repackager = sys.modules.get('cdx_toolkit.repackage.repackager')
    return repackager.source_threads() if repackager is not None else set()


def exit_cli(parser: ArgumentParser, status: int, message: str, running: set) -> None:
    """Exit with the status and message. If the source thread of this run still waits for its input (not one of
    the threads `running` before the run), Python could hang at exit (on Windows, closing stdin waits for a pending
    read of another thread): then exit without finalization; the outputs are complete or removed at this point."""
    if source_threads() - running:
        sys.stderr.write(message)
        logging.shutdown()
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(status)
    parser.exit(status, message)
