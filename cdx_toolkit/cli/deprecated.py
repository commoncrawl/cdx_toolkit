'''
Old commands: `iter` is a deprecated alias of `index` (same arguments); `size` and `warc` were removed and
print the equivalent new command line.
'''
import shlex
import sys
from typing import List, Optional, Tuple


DEPRECATED_COMMANDS = {'iter': 'index'}
REMOVED_COMMANDS = {'size': 'index --count', 'warc': 'index | cdxt repackage'}


DOCS_URL = 'https://github.com/commoncrawl/cdx_toolkit#command-line-tools'

# options of the old command line that take a value
VALUE_OPTIONS = {
    '--crawl', '--source', '--wb', '--limit', '--cc-mirror', '--cc-sort', '--from', '--to', '--filter',
    '--closest', '--fields', '--prefix', '--subprefix', '--size', '--creator', '--operator', '--url-fgrep',
    '--url-fgrepv', '--warc-download-prefix',
}
INDEX_OPTIONS = {
    '--crawl', '--limit', '--cc-mirror', '--cc-sort', '--from', '--to', '--filter', '--closest', '--fields',
    '--url-fgrep', '--url-fgrepv',
}
REPACKAGE_OPTIONS = {'--wb', '--prefix', '--subprefix', '--size', '--creator', '--operator', '--warc-download-prefix'}


def parse_old_args(args: List[str]) -> Tuple[List[Tuple[str, Optional[str]]], List[Tuple[int, str]]]:
    """Split a command line into (option, value) pairs and (position, value) of the positional arguments."""
    options, positionals = [], []
    tokens = iter(enumerate(args))
    for i, token in tokens:
        if token.startswith('-') and token != '-':
            if '=' in token:
                options.append(tuple(token.split('=', 1)))
            elif token in VALUE_OPTIONS:
                options.append((token, next(tokens, (None, ''))[1]))
            else:
                options.append((token, None))
        else:
            positionals.append((i, token))
    return options, positionals


def get_command(args: List[str]) -> Optional[str]:
    _, positionals = parse_old_args(args)
    return positionals[0][1] if positionals else None


def translate(args: List[str]) -> str:
    """Best-effort translation of an old size/warc command line to the new syntax."""
    options, positionals = parse_old_args(args)
    command, urls = positionals[0][1], [url for _, url in positionals[1:]]

    index, index_args, repackage_args = None, [], []
    for option, value in options:
        if option in ('--cc', '--ia'):
            index = option[2:]
        elif option == '--source':
            index = value
        elif option in ('-v', '-vv', '--verbose'):
            index_args.append(option)
        elif option == '--jsonl':
            index_args += ['--output-format', 'jsonl']
        elif option == '--csv':
            index_args += ['--output-format', 'csv']
        elif option in INDEX_OPTIONS:
            index_args += [option, value]
        elif option in REPACKAGE_OPTIONS:
            repackage_args += [option, value]
        # --get, --all-fields and --details have no equivalent (all fields are the default)

    if index is None:
        index = 'cc' if any(option == '--crawl' for option, _ in options) else 'INDEX'
    url_args = [arg for url in urls for arg in ('--url', url)]

    line = ['cdxt', 'index', index] + url_args + index_args
    if command == 'size':
        line.append('--count')
    elif command == 'warc':
        if index == 'ia' and '--wb' not in repackage_args:
            repackage_args += ['--wb', 'ia']
        line += ['|', 'cdxt', 'repackage'] + repackage_args
    return ' '.join(token if token == '|' else shlex.quote(token) for token in line)


def rewrite_deprecated_command(args: List[str]) -> List[str]:
    """Run `cdxt iter ...` exactly like `cdxt index ...` (same arguments), with a deprecation warning on stderr."""
    _, positionals = parse_old_args(args)
    position, command = positionals[0]
    new_command = DEPRECATED_COMMANDS[command]
    sys.stderr.write(
        f'cdxt: "{command}" is deprecated and will be removed in a future version, use "cdxt {new_command}" '
        f'(same arguments). See {DOCS_URL}\n'
    )
    return args[:position] + [new_command] + args[position + 1:]


def exit_removed_command(command: str, args: List[str]):
    message = f'cdxt: the "{command}" command has been removed, use "cdxt {REMOVED_COMMANDS[command]}" instead.\n'
    try:
        message += f'\nThe equivalent command is:\n\n  {translate(args)}\n'
    except Exception:  # pragma: no cover - the translation is only a hint
        pass
    message += f'\nSee "cdxt index --help", "cdxt repackage --help" or {DOCS_URL}\n'
    sys.stderr.write(message)
    raise SystemExit(2)
