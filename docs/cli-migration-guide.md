# Migrating to the new `cdxt` command line

The `cdxt` command line tool has a new design with two commands:

- **`cdxt index`** selects captures from an index and writes them as JSONL, CDXJ, CSV or TSV
  (replaces `cdxt iter` and `cdxt size`).
- **`cdxt repackage`** fetches the captures listed in an index file and writes them into WARC files
  (replaces `cdxt warc`, together with `cdxt index`).

The command lines of cdx_toolkit 0.9.39 and earlier (options before the command, the URL as a
positional argument, the `iter`, `size` and `warc` commands) still work, but they are deprecated and
will be removed in a future version. They run with the new code and print a suggested new command line
on stderr:

```
$ cdxt --cc --limit 10 iter 'commoncrawl.org/*'
cdxt: this command line uses the old syntax, which is deprecated and will be removed in a future version; suggested replacement: cdxt index cc --url 'commoncrawl.org/*' --limit 10 --output-format csv --fields status,timestamp,url (see https://github.com/commoncrawl/cdx_toolkit#command-line-tools)
status 200, timestamp 20250804103329, url https://www.commoncrawl.org/
...
```

The output of the old `iter` (text, `--jsonl`, `--csv`, the default fields `url,status,timestamp`),
`--get` and `size --details` are kept. The suggested replacement is not always exact: the new
commands have no text output (CSV is suggested instead), `--closest` always makes a single query, and
`cdxt index --count` is at most `--limit` and has no `--details`.

The old `warc` command now runs like `cdxt index ... | cdxt repackage --no-sort`, which changes:

- the WARC files are named `TEST-001.warc.gz`, `TEST-002.warc.gz`, ... (with `--prefix`/`--subprefix`:
  `PREFIX-SUBPREFIX-001.warc.gz`) instead of `TEST-000000.extracted.warc.gz`; WARC files with the same
  prefix must not exist yet
- the WARC records are copied byte-for-byte, without the added `WARC-Source-URI` and
  `WARC-Source-Range` headers
- no WARC file is written without captures
- the exit status is 1 if captures could not be fetched (the others are written)

`cdxt --cc iter --all-fields --csv` is a usage error (it failed with a traceback before).

## Quick reference

| Old | New |
|---|---|
| `cdxt --cc iter URL` | `cdxt index cc --url URL` |
| `cdxt --ia iter URL` | `cdxt index ia --url URL` |
| `cdxt --source CDX_API_URL iter URL` | `cdxt index CDX_API_URL --url URL` |
| `cdxt --crawl 3 iter URL` | `cdxt index cc --crawl 3 --url URL` |
| `cdxt --cc size URL` | `cdxt index cc --url URL --count` |
| `cdxt --cc warc URL` | `cdxt index cc --url URL \| cdxt repackage` |
| `cdxt --ia warc URL` | `cdxt index ia --url URL \| cdxt repackage --wb ia` |

## What changed

### All options go after the command

Before, the index options (`--cc`, `--limit`, `--from`, ...) had to come *before* the command and the
output options *after* it. Now all options go after the command, in any order. Only `-v`/`-vv`
and `--version` can also come before the command.

```sh
# old
cdxt --cc --limit 10 --from 2017 iter --csv 'commoncrawl.org/*'

# new
cdxt index cc --url 'commoncrawl.org/*' --limit 10 --from 2017 --output-format csv
```

### The index is a positional argument

`--cc`, `--ia` and `--source URL` are replaced by the first argument of `cdxt index`:

| Old | New |
|---|---|
| `--cc` | `cdxt index cc` |
| `--ia` | `cdxt index ia` |
| `--source https://web.archive.org/cdx/search/cdx` | `cdxt index https://web.archive.org/cdx/search/cdx` |
| `--crawl ...` (implied `--cc`) | `cdxt index cc --crawl ...` |

`--crawl` now has to be combined with `cc` explicitly; it cannot be used with other indexes.

### The URL is an option

The URL pattern moves from a positional argument to `--url`. `--url` can be repeated, and there
are two new ways to select captures:

| Option | Selects | Example |
|---|---|---|
| `--url PATTERN` | a URL pattern (as before) | `--url 'commoncrawl.org/*'`, `--url '*.commoncrawl.org'` |
| `--hostname HOST` | all captures of one exact hostname | `--hostname www.commoncrawl.org` |
| `--domain DOMAIN` | all captures of a domain and its subdomains | `--domain commoncrawl.org` |

They can be repeated and combined; a capture is selected if it matches any of them:

```sh
cdxt index cc --domain commoncrawl.org --url 'example.com/about*' --limit 100
```

### Output: file or stdout, format and fields

| Old | New |
|---|---|
| output always to stdout | stdout by default, or `-o/--output PATH` (local or remote, e.g. `s3://...`; gzipped if it ends with `.gz`) |
| `iter` (text: `status 200, timestamp ..., url ...`) | JSONL by default |
| `iter --jsonl` | default, or `--output-format jsonl` |
| `iter --csv` | `--output-format csv`, or `-o captures.csv` |
| — | `--output-format cdxj` / `tsv`, or `-o captures.cdxj` / `captures.tsv` |
| `--fields url,status,timestamp` (default) | `--fields url,status,timestamp` (default: all fields) |
| `--all-fields` | default |

The output format is detected from the `--output` file extension (`.jsonl`, `.json`, `.cdxj`,
`.cdx`, `.csv`, `.tsv`, each optionally with `.gz`). For other extensions, set `--output-format`.

The old human-readable text output is gone. The closest replacement is CSV with the old default fields:

```sh
# old
cdxt --cc --limit 10 iter 'commoncrawl.org/*'

# new
cdxt index cc --url 'commoncrawl.org/*' --limit 10 --fields url,status,timestamp --output-format csv
```

Note that the CSV columns now follow the order given in `--fields` (before, they were sorted
alphabetically) and lines end with `\n` instead of `\r\n`.

### `size` is now `index --count`

`--count` prints the same estimated number of captures as the old `size` command (based on the number
of index pages, so it is cheap but not exact).

```sh
# old
cdxt --cc size 'commoncrawl.org/*'

# new
cdxt index cc --url 'commoncrawl.org/*' --count
```

`index --count` has no `--details` (one line per index), and is at most `--limit`. Use `-v` to see which
indexes are queried; the deprecated `cdxt --cc size --details URL` still prints the lines.

### `warc` is now `index` + `repackage`

Creating WARC files is now two steps: `cdxt index` selects the captures, and `cdxt repackage`
fetches them and writes the WARC files. You can pipe one into the other, or save the captures to a
file first (handy to inspect them, or to repackage the same selection again later).

```sh
# old
cdxt --cc --limit 10 warc --prefix COMMONCRAWL 'commoncrawl.org/*'

# new: piped
cdxt index cc --url 'commoncrawl.org/*' --limit 10 | cdxt repackage --prefix COMMONCRAWL

# new: via a file
cdxt index cc --url 'commoncrawl.org/*' --limit 10 -o captures.jsonl
cdxt repackage captures.jsonl --prefix COMMONCRAWL
```

The `warc` options are split between the two commands:

| Old `warc` option | New |
|---|---|
| `--url-fgrep TEXT` | `cdxt index --url-fgrep TEXT` |
| `--url-fgrepv TEXT` | `cdxt index --url-fgrepv TEXT` |
| `--prefix` (default `TEST`), `--subprefix` | `cdxt repackage --prefix` (default `REPACKAGE`), `--subprefix` |
| `--size 1000000000` | `cdxt repackage --size 1000000000`, or human readable: `--size 1GB`, `--size 100MiB` |
| `--creator`, `--operator` | `cdxt repackage --creator`, `--operator` |
| `--warc-download-prefix` | `cdxt repackage --warc-download-prefix` |
| `--wb` (global option) | `cdxt repackage --wb` |
| — | `cdxt repackage --is-part-of`, `--description` (`warcinfo` record fields) |

**Common Crawl** captures contain their WARC location (`filename`, `offset`, `length`) and are
fetched from `--warc-download-prefix` (default `https://data.commoncrawl.org`; use
`s3://commoncrawl` in AWS us-east-1). Two things differ from the old `warc` command:

- **File names:** `<prefix>[-<subprefix>]-001.warc.gz`, `-002`, ... instead of
  `<prefix>[-<subprefix>]-000000.extracted.warc.gz`, `-000001`, ...
- **No empty WARC file** is written when the index selected no captures.
- **Records are copied byte-for-byte** from the source WARC files. Before, they were parsed and
  re-written with added `WARC-Source-URI` and `WARC-Source-Range` headers; now the records (and
  their digests) are exactly the originals.

The records are also sorted by WARC file and offset before fetching (`--no-sort` keeps the order of
the index), and can be fetched in parallel (`--parallel-readers`, `--processes`).

**Internet Archive** captures have no WARC location; they are replayed from the wayback machine.
Before, `--ia` did this automatically. Now pass `--wb ia` (or the URL of another wayback machine).
They are written like Common Crawl captures, to `<prefix>[-<subprefix>]-001.warc.gz`, ... (before:
`-000000.extracted.warc.gz`, ...), and can be fetched with `--parallel-readers`:

```sh
# old
cdxt --ia --limit 10 warc 'commoncrawl.org/*'

# new
cdxt index ia --url 'commoncrawl.org/*' --limit 10 | cdxt repackage --wb ia

# old: other CDX server and wayback machine
cdxt --source https://web.archive.org/cdx/search/cdx --wb https://web.archive.org/web warc 'commoncrawl.org/*'

# new
cdxt index https://web.archive.org/cdx/search/cdx --url 'commoncrawl.org/*' \
    | cdxt repackage --wb https://web.archive.org/web
```

### Other option changes

| Old | New |
|---|---|
| `--get` | removed; `cdxt index` always picks the right query type (`--closest` uses a single query) |
| `--closest TS --get` | `--closest TS` |
| `--from`, `--to`, `--filter`, `--limit` | unchanged (after the command) |
| `--cc-mirror`, `--cc-sort` | unchanged (after the command); `--cc-sort` now actually takes effect |
| `-v`, `-vv` | unchanged; before or after the command |

`--limit` is the maximum number of captures read from the index. As with the old `warc --url-fgrep`,
the limit applies *before* `--url-fgrep`/`--url-fgrepv`, so the output can have fewer records.

Usage errors (for example a missing `--url`, or `--crawl` with `ia`) now print a short message and
exit with code 2 instead of showing a Python traceback.

## More examples

```sh
# most recent 3 crawls
# old: cdxt --crawl 3 --limit 10 iter 'commoncrawl.org/*'
cdxt index cc --crawl 3 --url 'commoncrawl.org/*' --limit 10

# a specific crawl
# old: cdxt --crawl CC-MAIN-2024-33 iter 'commoncrawl.org/*'
cdxt index cc --crawl CC-MAIN-2024-33 --url 'commoncrawl.org/*'

# time range at the Internet Archive
# old: cdxt --ia --limit 1 --from 2008 --to 200906302359 iter 'commoncrawl.org/*'
cdxt index ia --url 'commoncrawl.org/*' --limit 1 --from 2008 --to 200906302359

# filters
# old: cdxt --cc --filter '!=status:200' --filter '!=status:404' iter 'commoncrawl.org/*'
cdxt index cc --url 'commoncrawl.org/*' --filter '!=status:200' --filter '!=status:404'

# selected fields as CSV
# old: cdxt --cc --limit 10 iter --fields=digest,length,offset --csv 'commoncrawl.org/*'
cdxt index cc --url 'commoncrawl.org/*' --limit 10 --fields digest,length,offset -o captures.csv

# closest capture
# old: cdxt --cc --limit 3 --get --closest=20170615 iter 'commoncrawl.org/*'
cdxt index cc --url 'commoncrawl.org/*' --limit 3 --closest 20170615

# WARC without robots.txt, with metadata, in 100 MB files
# old: cdxt --cc --limit 100 warc --prefix CC --subprefix BLOG --size 100000000 \
#          --creator me --url-fgrepv robots.txt 'commoncrawl.org/*'
cdxt index cc --url 'commoncrawl.org/*' --limit 100 --url-fgrepv robots.txt \
    | cdxt repackage --prefix CC --subprefix BLOG --size 100MB --creator me

# save the selection to S3, repackage it later (needs cdx_toolkit[s3])
cdxt index cc --domain commoncrawl.org --crawl CC-MAIN-2024-33 -o s3://my-bucket/captures.jsonl.gz
cdxt repackage s3://my-bucket/captures.jsonl.gz --prefix s3://my-bucket/warcs/COMMONCRAWL
```

## New: the columnar URL index

Besides the CDX API, `cdxt index` can query Common Crawl's columnar URL index (parquet) with DuckDB
or AWS Athena. This was not possible with the old commands:

```sh
# DuckDB over s3://commoncrawl/cc-index/table/cc-main/warc/ (needs AWS credentials and cdx_toolkit[duckdb])
cdxt index cc-url-index-s3 --domain commoncrawl.org --crawl CC-MAIN-2024-33 -o captures.jsonl

# your own SQL query (examples/homepages.sql in the cdx_toolkit repository)
cdxt index cc-url-index-s3 --sql examples/homepages.sql --yes -o homepages.csv

# AWS Athena (needs cdx_toolkit[athena])
cdxt index 'athena://athena.us-east-1.amazonaws.com/ccindex?S3OutputLocation=s3://my-bucket/athena/' \
    --hostname commoncrawl.org --crawl CC-MAIN-2024-33 -o captures.jsonl
```

See the README for all index types.

## Users of the `feat/warc-range-sources` branch

The `repackage` command of the unreleased `feat/warc-range-sources` branch (#73) is split into
`cdxt index` (selecting the records) and `cdxt repackage` (fetching them):

| Branch | New |
|---|---|
| `cdxt --crawl C repackage --target-source sql --engine athena --athena-database D --athena-s3-output S3 --hostnames H ...` | `cdxt index 'athena://athena.us-east-1.amazonaws.com/D?S3OutputLocation=S3' --crawl C --hostname H -o captures.csv` + `cdxt repackage captures.csv ...` |
| `... --engine duckdb --duckdb-index-path PATH` | `cdxt index PATH --index-backend duckdb ...` (or `cc-url-index-s3`, `cc-url-index-hf`) |
| `--hostnames H1 H2`, `--domains D1 D2` | `--hostname H1 --hostname H2`, `--domain D1 --domain D2` |
| `--query SQL`, `--query-file FILE` | `cdxt index ... --sql FILE` (`--sql -` reads stdin) |
| — | new for SQL indexes: `--from`/`--to` (`fetch_time`), `--language` (primary content language, ISO 639-3), Parquet output (`-o captures.parquet`, `cdx_toolkit[parquet]`) |
| `--confirm-cost` | `cdxt index --yes` |
| `--target-source cdx --cdx-path PATH [--cdx-glob GLOB]` | `cdxt repackage 'PATH/GLOB'` (CDX files are read directly) |
| `--target-source csv --csv-path FILE` | `cdxt repackage FILE` |
| `--range-jobs-output FILE --no-fetch` | `cdxt index ... -o FILE` |
| `--csv-self-contained` (`warc_url` column) | removed; `warc_filename` (or `filename`) may hold a full URL (`s3://`, `https://`, `hf://`), which is used as it is, otherwise it is joined with `--warc-download-prefix` |
| `--limit N` (global option) | `cdxt index --limit N` (repackage does not filter) |
| `--parallel N`, `--parallel_readers N` | `cdxt repackage --parallel-readers N` |
| `--no-sort-ranges` | `cdxt repackage --no-sort` |
| `--write-paths-as-metadata-records F1 F2` | `cdxt repackage --add-metadata-record F1 --add-metadata-record F2` |
| `--log_every_n N` | `cdxt repackage --log-every-n N` |
| `--keep-shards` | removed; with `--processes`, all processes write directly into one `-001`, `-002`, ... series (`--subprefix` now works too) |
| `--hf-reader fsspec\|cdn` | removed; `hf://` WARCs are always read from the CDN, with one rate-limited `/resolve/` request per WARC file instead of per record (`fsspec` was ~10x slower and dropped records under load in the benchmarks) |
| `--processes`, `--warc-download-prefix`, `--prefix`, `--subprefix`, `--size`, `--creator`, `--operator`, `--is-part-of`, `--description` | unchanged (`--size` also accepts e.g. `1GB`) |
| `cdxt filter_cdx INPUT_BASE FILTER_FILE OUTPUT_BASE --input-glob GLOB --parallel N` | `cdxt index 'INPUT_BASE/GLOB' --url-file FILTER_FILE -o OUTPUT_BASE --processes N` (one output per CDX file). Add `--url-file-type domain` to match the entries like `--filter-type url` did (`example.com` = the domain with subdomains, entries with a path = URL prefixes); without it, the entries are URL patterns like `--url` |
| `--filter-type surt`, `--overwrite`, `--limit` (per file) | removed; the output directory has to be new or empty, `--limit` only works with a single output |

## Python API

`cdx_toolkit.CDXFetcher`, `cdx.iter()`, `cdx.get_size_estimate()` and `capture.content` have not
changed. New: the Python equivalents of the new commands, which run the same code as the command line
(see the README):

```python
from cdx_toolkit.indexes import IndexQuery, open_index
from cdx_toolkit.repackage import repackage

index = open_index('cc')                                     # cdxt index cc
query = IndexQuery(urls='commoncrawl.org/*', limit=10)       #   --url 'commoncrawl.org/*' --limit 10
repackage(index.records(query), prefix='COMMONCRAWL')        # | cdxt repackage --prefix COMMONCRAWL
```

`cdx_toolkit.warc.get_writer()` (`CDXToolkitWARCWriter`) is deprecated and will be removed in
cdx_toolkit 1.0; `repackage()` replaces it.
