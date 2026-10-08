'''
Reading and writing index records (captures) as JSONL, CDXJ, CSV, TSV or Parquet.

Paths can be local or remote (via fsspec, if installed), optionally gzipped; "-" is stdin/stdout.
Parquet needs the optional pyarrow dependency (cdx_toolkit[parquet]) and a file path.
'''
import contextlib
import csv
import gzip
import itertools
import json
import re
import sys
from typing import IO, Iterable, Iterator, List, Mapping, Optional

try:
    import fsspec
except ImportError:  # pragma: no cover - exercised in minimal installs
    fsspec = None


TEXT_FORMATS = ('jsonl', 'cdxj', 'csv', 'tsv')
FORMATS = TEXT_FORMATS + ('parquet',)

# Common Crawl's index files are named *.cdx(.gz) but use the CDXJ format (crawl index JSON)
FORMAT_ALIASES = {'cdx': 'cdxj'}
# formats for the command line, including the aliases
FORMAT_CHOICES = ('jsonl', 'cdx', 'cdxj', 'csv', 'tsv', 'parquet')

# records per Parquet row group
PARQUET_BATCH_SIZE = 100_000

EXTENSION_FORMATS = {
    '.jsonl': 'jsonl',
    '.json': 'jsonl',
    '.cdxj': 'cdxj',
    '.cdx': 'cdxj',
    '.csv': 'csv',
    '.tsv': 'tsv',
    '.parquet': 'parquet',
}

# column names of the columnar index (and range-job CSVs) -> CDX field names
FIELD_ALIASES = {
    'warc_filename': 'filename',
    'warc_record_offset': 'offset',
    'warc_record_length': 'length',
}

CDXJ_LINE = re.compile(r'^\S+ \d{4,14} \{')


def canonical_format(fmt: Optional[str]) -> Optional[str]:
    """'cdx' -> 'cdxj'; other formats unchanged."""
    return FORMAT_ALIASES.get(fmt, fmt)


def format_from_path(path: str) -> Optional[str]:
    """Infer the record format from a file extension (ignoring .gz), None if unknown."""
    if path == '-':
        return None
    path = path.lower()
    if path.endswith('.gz'):
        path = path[:-3]
    for extension, fmt in EXTENSION_FORMATS.items():
        if path.endswith(extension):
            return fmt
    return None


def open_text(path: str, mode: str) -> IO:
    """Open a local or remote text file; '-' means stdin/stdout (which is not closed)."""
    if path == '-':
        return contextlib.nullcontext(sys.stdout if 'w' in mode else sys.stdin)
    if fsspec is not None:
        return fsspec.open(path, mode + 't', compression='infer', newline='')
    if '://' in path:
        raise RuntimeError(f'Reading or writing {path} requires fsspec. Install cdx_toolkit[s3].')
    if path.endswith('.gz'):
        return gzip.open(path, mode + 't', newline='')
    return open(path, mode, newline='')


class RecordWriter:
    """Write records in one of TEXT_FORMATS; `fields` selects and orders the fields (default: all)."""

    def __init__(self, fh: IO, fmt: str, fields: Optional[List[str]] = None):
        fmt = canonical_format(fmt)
        if fmt not in TEXT_FORMATS:
            raise ValueError(f'unknown format {fmt!r}')
        self.fh = fh
        self.fmt = fmt
        self.fields = fields
        self._csv_writer = None

    def write(self, record: Mapping) -> None:
        record = dict(record)
        selected = {k: record[k] for k in self.fields if k in record} if self.fields else record

        if self.fmt == 'jsonl':
            self.fh.write(json.dumps(selected, sort_keys=True, default=str) + '\n')
        elif self.fmt == 'cdxj':
            for key in ('urlkey', 'timestamp'):
                if key not in record:
                    raise ValueError(f'cannot write CDXJ: record has no {key!r} field')
            data = {k: v for k, v in selected.items() if k not in ('urlkey', 'timestamp')}
            self.fh.write(f'{record["urlkey"]} {record["timestamp"]} {json.dumps(data, default=str)}\n')
        else:
            if self._csv_writer is None:
                self._csv_writer = csv.DictWriter(
                    self.fh,
                    fieldnames=self.fields or list(record.keys()),
                    delimiter='\t' if self.fmt == 'tsv' else ',',
                    lineterminator='\n',
                    extrasaction='ignore',
                )
                self._csv_writer.writeheader()
            self._csv_writer.writerow(selected)


def is_glob(path: str) -> bool:
    # in http(s) URLs, ? starts the query string
    chars = '*[' if path.lower().startswith(('http://', 'https://')) else '*?['
    return path != '-' and any(c in path for c in chars)


def check_glob(path: str) -> None:
    """Globs need a filesystem that can list files: not http(s)."""
    if is_glob(path) and path.lower().startswith(('http://', 'https://')):
        raise ValueError(
            f'globs do not work with http(s) URLs ({path}), because they cannot list files; '
            'use e.g. s3://commoncrawl/... (needs AWS credentials) or one URL per file'
        )


def expand_paths(path: str) -> List[str]:
    """A local or remote path, or all files matching a glob (e.g. s3://bucket/indexes/*.cdx.gz)."""
    if not is_glob(path):
        return [path]
    check_glob(path)
    if fsspec is None:
        import glob

        if '://' in path:
            raise RuntimeError(f'Reading {path} requires fsspec. Install cdx_toolkit[s3].')
        return sorted(glob.glob(path))
    fs, fs_path = fsspec.url_to_fs(path)
    return [fs.unstrip_protocol(p) for p in sorted(fs.glob(fs_path))]


def read_records(path: str, fmt: Optional[str] = None) -> Iterator[dict]:
    """Read records from a file, all files matching a glob, or stdin ('-').

    The format is detected from the first line of each file if not given."""
    fmt = canonical_format(fmt)
    paths = expand_paths(path)
    if not paths:
        raise FileNotFoundError(f'no files found for {path}')
    for p in paths:
        if fmt == 'parquet' or (fmt is None and format_from_path(p) == 'parquet'):
            yield from read_parquet(p)
        else:
            yield from _read_records(p, fmt)


def _read_records(path: str, fmt: Optional[str] = None) -> Iterator[dict]:
    with open_text(path, 'r') as fh:
        lines = iter(fh)
        first_line = next(lines, None)
        if first_line is None:
            return
        lines = itertools.chain([first_line], lines)
        fmt = fmt or sniff_format(first_line)

        if fmt == 'jsonl':
            records = (json.loads(line) for line in lines if line.strip())
        elif fmt == 'cdxj':
            records = (parse_cdxj_line(line) for line in lines if line.strip())
        elif fmt in ('csv', 'tsv'):
            records = csv.DictReader(lines, delimiter='\t' if fmt == 'tsv' else ',')
        else:
            raise ValueError(f'unknown format {fmt!r}')

        for record in records:
            yield normalize_field_names(record)


def sniff_format(line: str) -> str:
    if line.lstrip().startswith('{'):
        return 'jsonl'
    if CDXJ_LINE.match(line):
        return 'cdxj'
    if '\t' in line:
        return 'tsv'
    return 'csv'


def parse_cdxj_line(line: str) -> dict:
    urlkey, timestamp, data = line.split(' ', 2)
    record = json.loads(data)
    record.setdefault('urlkey', urlkey)
    record.setdefault('timestamp', timestamp)
    return record


def normalize_field_names(record: dict) -> dict:
    for alias, name in FIELD_ALIASES.items():
        if alias in record and name not in record:
            record[name] = record.pop(alias)
    return record


def import_pyarrow():
    try:
        import pyarrow
        import pyarrow.parquet
    except ImportError:
        raise RuntimeError(
            'Parquet files need the optional pyarrow dependency. Install cdx_toolkit[parquet].'
        ) from None
    return pyarrow


def write_parquet(records: Iterable[Mapping], path: str, fields: Optional[List[str]] = None) -> int:
    """Write records to a Parquet file (all columns are strings), returns the number of records written.

    Without `fields`, the columns are the fields of the first batch of records."""
    if path == '-':
        raise ValueError('Parquet output cannot be written to stdout, please set --output')
    pa = import_pyarrow()

    n = 0
    schema = None
    writer = None
    with fsspec.open(path, 'wb') if fsspec is not None else open(path, 'wb') as fh:
        try:
            batch = []
            for record in itertools.chain(records, [None]):
                if record is not None:
                    batch.append(dict(record))
                    if len(batch) < PARQUET_BATCH_SIZE:
                        continue
                if not batch and schema is not None:
                    break
                if schema is None:
                    columns = fields or list(dict.fromkeys(k for r in batch for k in r))
                    schema = pa.schema([(c, pa.string()) for c in columns])
                    writer = pa.parquet.ParquetWriter(fh, schema)
                table = pa.table(
                    {c: [None if r.get(c) is None else str(r[c]) for r in batch] for c in schema.names},
                    schema=schema,
                )
                writer.write_table(table)
                n += len(batch)
                batch = []
        finally:
            if writer is not None:
                writer.close()
    return n


def read_parquet(path: str) -> Iterator[dict]:
    pa = import_pyarrow()
    with fsspec.open(path, 'rb') if fsspec is not None else open(path, 'rb') as fh:
        for batch in pa.parquet.ParquetFile(fh).iter_batches():
            for record in batch.to_pylist():
                yield normalize_field_names({k: v for k, v in record.items() if v is not None})


def write_records(records: Iterable[Mapping], path: str, fmt: str, fields: Optional[List[str]] = None) -> int:
    """Write records to a file or stdout ('-'), returns the number of records written."""
    fmt = canonical_format(fmt)
    if fmt == 'parquet':
        return write_parquet(records, path, fields)
    n = 0
    with open_text(path, 'w') as fh:
        writer = RecordWriter(fh, fmt, fields)
        for record in records:
            writer.write(record)
            n += 1
    return n
