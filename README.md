# cdx_toolkit

[![build](https://github.com/commoncrawl/cdx_toolkit/actions/workflows/ci.yaml/badge.svg)](https://github.com/commoncrawl/cdx_toolkit/actions/workflows/ci.yaml) [![coverage](https://codecov.io/gh/commoncrawl/cdx_toolkit/graph/badge.svg?token=M1YJB998LE)](https://codecov.io/gh/commoncrawl/cdx_toolkit) [![Apache License 2.0](https://img.shields.io/github/license/commoncrawl/cdx_toolkit.svg)](LICENSE)

cdx_toolkit is a set of tools for working with CDX indices of web
crawls and archives, including those at the Common Crawl Foundation
(CCF) and those at the Internet Archive's Wayback Machine.

Common Crawl uses Ilya Kreymer's pywb to serve the CDX API, which is
somewhat different from the Internet Archive's CDX API server.
cdx_toolkit hides these differences as best it can. cdx_toolkit also
knits together the monthly Common Crawl CDX indices into a single,
virtual index.

Finally, cdx_toolkit allows extracting archived pages from CC and IA
into WARC files. If you're looking to create subsets of CC or IA data
and then further process them, this is a feature you'll find useful.

## Installing

```
$ pip install cdx_toolkit
```

or clone this repo and use `pip install .`

Optional extras:

```
$ pip install 'cdx_toolkit[s3]'      # S3 input/output and WARC downloads from s3://
$ pip install 'cdx_toolkit[duckdb]'  # query the columnar URL index with DuckDB
$ pip install 'cdx_toolkit[athena]'  # query the columnar URL index with AWS Athena
$ pip install 'cdx_toolkit[hf]'      # Hugging Face Storage Buckets (hf://)
$ pip install 'cdx_toolkit[parquet]' # Parquet output (and input of cdxt repackage)
$ pip install 'cdx_toolkit[all]'     # install all optional dependencies
```

### Settings

The AWS settings of S3, Athena and DuckDB requests, and the rate limits and retries of HTTP requests to
Common Crawl and the Internet Archive (with their environment variables), are described in
[docs/settings.md](docs/settings.md).

## Command-line tools

The `cdxt` command line tool has two commands: `index` selects captures
from an index, and `repackage` fetches the selected captures and writes
them into new WARC files.

```
$ cdxt index cc --url 'commoncrawl.org/*' --limit 10  # returns the most recent year
$ cdxt index cc --url 'commoncrawl.org/*' --crawl 3 --limit 10  # returns the most recent 3 crawls
$ cdxt index cc --url 'commoncrawl.org/*' --limit 10 --filter '=status:200'
$ cdxt index cc --domain commoncrawl.org --limit 10 -o captures.csv
$ cdxt index cc --url 'commoncrawl.org/*' --count  # estimated number of captures

$ cdxt index ia --url 'commoncrawl.org/*' --limit 10  # will show the beginning of IA's crawl

$ cdxt index cc-url-index-s3 --domain commoncrawl.org --crawl CC-MAIN-2024-33 -o captures.csv  # DuckDB
$ cdxt index cc-url-index-s3 --sql examples/homepages.sql --yes -o captures.csv

$ cdxt index cc --url 'commoncrawl.org/*' --limit 10 -o captures.jsonl
$ cdxt repackage captures.jsonl --prefix COMMONCRAWL
$ cdxt index ia --url 'commoncrawl.org/*' --limit 10 | cdxt repackage --wb ia --prefix IA
```

Usage guides with examples for different setups:

- [HTTP](docs/usage/http.md): the CDX APIs and Common Crawl's HTTP server, no account needed
- [AWS: Athena and S3](docs/usage/aws-athena-s3.md): the columnar URL index with Athena or DuckDB, and WARC records from `s3://commoncrawl`
- [Hugging Face](docs/usage/huggingface.md): the Storage Bucket `hf://buckets/commoncrawl/commoncrawl`, and HF Jobs

### cdxt index

`cdxt index INDEX` takes one of these indexes. How it is queried is detected from INDEX, or set with
`--index-backend cdx-api|cdx-files|duckdb|athena` (e.g. `cdx-files` for a CDX file at an `https://`
URL without file extension):

| INDEX | Index |
|---|---|
| `cc`, `ia` | Common Crawl or Internet Archive CDX API |
| `https://...` (no file extension) | another CDX API server, e.g. `https://web.archive.org/cdx/search/cdx` |
| `cc-url-index-s3` | Common Crawl's [columnar URL index](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format) on S3 (`s3://commoncrawl/cc-index/table/cc-main/warc/`), queried with DuckDB; needs AWS credentials and `cdx_toolkit[duckdb]` |
| `cc-url-index-hf` | the same index on Hugging Face (`hf://buckets/commoncrawl/commoncrawl/cc-index/table/cc-main/warc/`, recent crawls only), queried with DuckDB; needs `cdx_toolkit[duckdb,hf]`. Much slower than S3 |
| `*.parquet`, or a hive-partitioned directory with `--index-backend duckdb` | another columnar index, local or remote, queried with DuckDB |
| `athena://athena.us-east-1.amazonaws.com/ccindex?S3OutputLocation=s3://bucket/path/` | the columnar URL index queried with AWS Athena (database `ccindex`, table `ccindex`; add `&table=...` for another table); needs `cdx_toolkit[athena]` |
| other paths, e.g. `'s3://commoncrawl/cc-index/collections/CC-MAIN-2024-30/indexes/cdx-*.gz'` | CDX files (one capture per line, optionally gzipped), local or remote, a single file or a glob |

Captures are selected with `--url` (a URL pattern like `example.com/*`
or `*.example.com`, or an exact URL), `--hostname` (an exact hostname like
`www.example.com`) and `--domain` (a domain including its subdomains).
These can be repeated and combined; a capture is selected if it
matches any of them. All other options, like `--crawl`, `--from`,
`--to`, `--filter` and `--url-fgrep`, narrow the selection down.
Options an index does not support (e.g. `--filter` with DuckDB, or `--cc-mirror`
and `--cc-sort` with any other index than `cc`) are rejected; `--yes` is
accepted by every index (only the SQL indexes ask for confirmation).
For CDX files and the SQL indexes, `--url` patterns are matched against SURT
keys (`url_surtkey` in the columnar index), like the CDX API does: `www.` is
ignored, so `--url 'example.com/*'` also matches `www.example.com`, while
`--hostname example.com` does not.

CDX files also support `--url-file PATH`: URL patterns (with the same syntax
as `--url`, e.g. `*.example.com` for a domain) from a file with one pattern per
line, or from the `url` column (or `--url-column NAME`) of a CSV, TSV, JSONL or
Parquet file. For lists of hosts, use `--url-file-type domain`: then an entry
like `example.com` matches the domain and its subdomains, and an entry with a
path like `example.com/reports` all URLs starting with it. The patterns are matched against the SURT keys of the CDX lines,
so long URL lists are fast. For DuckDB and Athena, join the index with a table
of the URLs in a custom `--sql` query instead.

With a glob of CDX files and an `-o` path without file extension (or ending
with `/`), `cdxt index` writes one output per CDX file, with the same relative
path and the selected lines unchanged (e.g. to filter a whole crawl's index for
a URL list), and `--processes N` filters N files in parallel. No file is
written for CDX files without matches. The directory has to be new or empty, so
that no output of an earlier run is mixed with this one:

```
$ cdxt index 's3://commoncrawl/cc-index/collections/CC-MAIN-2024-30/indexes/cdx-*.gz' \
    --url-file eot-hosts.txt --url-file-type domain -o s3://my-bucket/eot/CC-MAIN-2024-30/indexes --processes 8
$ cdxt repackage 's3://my-bucket/eot/CC-MAIN-2024-30/indexes/cdx-*.gz' --prefix EOT-2024
```

An `-o` path with an unknown extension (e.g. `out.txt`) needs `--output-format`.
`cdxt index` refuses an output that would overwrite one of the files the query
reads (the CDX or Parquet files, the `--url-file`).

The SQL indexes (DuckDB, Athena) also support `--language CODE` (repeatable):
captures whose primary content language (the first of the `languages` field)
is one of the given [ISO 639-3](https://iso639-3.sil.org/) codes, e.g. `eng`,
`deu` or `fra` (three letters; e.g. `en` is rejected).

The columnar index can also be queried with your own SQL: `--sql query.sql`
(or `--sql -` for stdin) runs the query as it is, instead of `--hostname`,
`--domain`, `--crawl`, `--from`, `--to`, `--language` and `--limit`; the query names the
Parquet files it reads, see [examples/homepages.sql](examples/homepages.sql), and the
[usage guides](docs/usage/) for more. Without `--sql`, the query selects the
CDX fields (`url`, `timestamp`, `status`, `mime`, `digest`, `filename`,
`offset`, `length`, `languages`, ... and `crawl`) for the given hosts and
domains; `--dry-run` prints it without running it.

SQL queries that are not restricted to at most 10 crawls (with `--crawl`,
or any `--sql` query) ask for confirmation first, because Athena bills per
TB scanned and DuckDB would read a lot of data. Pass `--yes` to skip the
question, e.g. in scripts.

The captures are written to stdout or to the file given with
`-o/--output` (local or remote, e.g. `s3://...` with `cdx_toolkit[s3]`;
gzipped if the name ends with `.gz`). The output format is JSONL, CDXJ,
CSV, TSV or Parquet (needs `cdx_toolkit[parquet]`, not for stdout), detected
from the file extension or set with `--output-format`; stdout defaults to
JSONL. `cdx` is the same as `cdxj`: Common Crawl's index files have the `.cdx`
extension but use the CDXJ format (crawl index JSON). Use `--fields url,status,timestamp` to select fields; by default all
fields are written.

`--count` prints the number of captures instead. For the CDX API this is
a cheap estimate based on the number of index pages (for `--hostname`, of the
host with and without `www.`, which the CDX API does not distinguish; the
captures of the other one are dropped when they are read); for CDX files it is
an exact count, and for SQL indexes an exact `count(*)`.

If you don't specify the crawls or dates you're interested in, queries
of `cc` are limited to the CC indexes of the most recent year, to
prevent overly-large queries. To exceed this limit, use `--crawl` or
`--from` and `--to`. There is no default `--limit`.

If it seems like nothing is happening, add `-v` or `-vv`:

```
$ cdxt -vv index cc --url 'commoncrawl.org/*' --count
```

#### Selecting particular CCF crawls

Common Crawl's data is divided into "crawls", which were yearly at the
start, and are currently done monthly. There are over 100 of them.
[You can find details about these crawls here.](https://data.commoncrawl.org/crawl-data/index.html)

Unlike some web archives, CCF doesn't have a single CDX index that
covers all of these crawls -- we have 1 index per crawl. The way
you ask for a particular crawl is:

```
$ cdxt index cc --crawl CC-MAIN-2024-33 --url 'commoncrawl.org/*'
```

- `--crawl CC-MAIN-2024-33` is a single crawl.
- `--crawl 3` is the latest 3 crawls.
- `--crawl CC-MAIN-2018` will match all of the crawls from 2018.
- `--crawl CC-MAIN-2018,CC-MAIN-2019` will match all of the crawls from 2018 and 2019.

CCF also has a hive-sharded parquet index (called the columnar index)
that covers all of our crawls. Querying broad time ranges is much
faster with the columnar index. You can find more information about
this index at [the blog post about it](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format).

The Internet Archive cdx index is organized as a single crawl that goes
from the very beginning until now. That's why there is no `--crawl` for
`ia`, and no default time range: without `--from` and `--to`, a query of
`ia` starts at the beginning of IA's crawl.

#### Selecting by time

In most cases you'll probably use --crawl to select the time range for
Common Crawl queries, but for the Internet Archive you'll need to specify
a time range like this:

```
$ cdxt index ia --limit 1 --from 2008 --to 200906302359 --url 'commoncrawl.org/*'
```

In this example the time range starts at the beginning of 2008 and
ends on June 30, 2009 at 23:59. All times are in UTC. If you do not
specify a time range (and also don't use `--crawl`), `cc` queries get the
most recent year, and `ia` queries all years.

#### Filter syntax

Filters can be used to limit captures to a subset of the results.

Any field name in the output of `cdxt index` can be used in a
filter.  These field names are appropriately renamed if the source is
'ia'.  The different syntax of filter modifiers for 'ia' and 'cc' is
not fully abstracted away by cdx_toolkit.

The basic syntax of a filter is `[modifier]field:expression`, for
example `=status:200` or `!=status:200`.

'cc'-style filters (pywb) come in six flavors: substring match, exact
string, full-match regex, and their inversions. These are indicated by
a modifier of nothing, '=', '\~', '!', '!=', and '!\~', respectively.

'ia'-style filters (Wayback/OpenWayback) come in two flavors, a full-match
regex and an inverted full-match regex: 'status:200' and '!status:200'

Multiple filters will be combined with AND. For example, to limit
captures to those which do not have status 200 and do not have status 404,

```
$ cdxt index cc --filter '!=status:200' --filter '!=status:404' ...
```

Note that filters that discard large numbers of captures put a high
load on the CDX server -- for example, a filter that returns just a
few captures from a domain that has tens of millions of captures is
likely to run very slowly and annoy the owner of the CDX server.

See https://github.com/webrecorder/pywb/wiki/CDX-Server-API#filter (pywb)
and https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server#filtering (wayback)
for full details of filter modifiers.

### cdxt repackage

`cdxt repackage INDEX_PATH` reads captures written by `cdxt index` (a
local or remote file, a glob like `s3://bucket/indexes/*.cdx.gz`, or stdin
if no path or `-` is given) and writes them into new WARC files. The input
format (JSONL, CDXJ, CSV or TSV) is detected from the first line, so Common
Crawl's CDX files can be used directly; `*.parquet` files are read as Parquet.
Use `--input-format` to set the format instead.
The columnar index column names (`warc_filename`, `warc_record_offset`,
`warc_record_length`) work too. A `filename` (or `warc_filename`) that is a
full URL (`s3://...`, `https://...`, `hf://...`) is used as it is, otherwise
it is relative to `--warc-download-prefix`.

Captures with a WARC location (the `filename`, `offset` and `length`
fields, e.g. all Common Crawl captures) are copied byte-for-byte from
`--warc-download-prefix`: `https://data.commoncrawl.org` (default),
`s3://commoncrawl` (fastest in AWS us-east-1, needs `cdx_toolkit[s3]`) or
`hf://buckets/commoncrawl/commoncrawl` (needs `cdx_toolkit[hf]`). They are
written to `<prefix>[-<subprefix>]-001.warc.gz`, `-002`, ..., where the
prefix can be a local path, `s3://...` or `hf://buckets/...` (default:
`REPACKAGE` in the current directory, e.g. `--prefix out/CC-HOMEPAGES` writes
`out/CC-HOMEPAGES-001.warc.gz`). No file is written when there are no
captures. The captures are
sorted by WARC file and offset first (for faster reads); `--no-sort` keeps
the input order and streams large inputs without keeping them in memory.

Reads from `hf://` count against the Hugging Face Hub's
[Resolvers rate limit](https://huggingface.co/docs/hub/en/rate-limits) with one
request per WARC file (the records themselves are read from the CDN), so set
`HF_TOKEN` (anonymous access has the lowest limit).

For large jobs, use `--parallel-readers N` (concurrent reads per process)
and `--processes N` (e.g. the number of vCPUs). With several processes,
the captures are split by WARC file across the processes, which write into
one shared `-001`, `-002`, ... series (which process writes which file
depends on timing; each file is a complete WARC).

Captures without a WARC location, like Internet Archive captures, are
replayed from the wayback machine given with `--wb` (`ia` is a shortcut for
`https://web.archive.org/web`) and written to the same
`<prefix>[-<subprefix>]-001.warc.gz`, ... series, also with `--parallel-readers` and
`--add-metadata-record` (`--processes` cannot be used with `--wb`: the requests to the wayback machine
are rate-limited per process). A replay needs the `url` (http or https), `timestamp` and `status` of
each capture. Common Crawl captures (`filename` `crawl-data/...`) are refused with `--wb`: they are
copied from their WARC files, while a replay would get another archive's capture.

In both cases, if WARC files with the same prefix and subprefix exist already
(e.g. from an earlier run), `cdxt repackage` stops before fetching (exit
status 2) instead of overwriting them or adding to them: use another
`--prefix` or `--subprefix`.

Use `--size` (e.g. `100MB` or `1GiB`) to set the target size of each WARC
file, `--creator`, `--operator`, `--is-part-of` and `--description` to fill
in the `warcinfo` record, and `--add-metadata-record FILE` to add a file
(e.g. the URL list of the job) as a metadata record to each WARC file.

A capture that cannot be fetched (after retries) does not stop the job: the
other captures are still written, and at the end `cdxt repackage` logs how
many captures were requested, written and failed, and exits with status 1.
A failure to write the output stops the job at once, with `--processes` all
processes; the WARC files being written are removed (or their S3 uploads
aborted), the files before them are complete. A process that has not stopped
after 60 seconds is terminated, which can leave an incomplete S3 multipart
upload (an S3 lifecycle rule for incomplete uploads removes it).

### Migrating from the old commands

The command lines of cdx_toolkit 0.9.39 (`cdxt --cc iter ...`, `size`,
`warc`) still work, but are deprecated and will be removed in a future
version; they print a suggested new command line. The old `warc` now writes
`TEST-001.warc.gz` and exits with status 1 if captures could not be fetched.
See the [migration guide](docs/cli-migration-guide.md) for all changes and
more examples.

| Old | New |
|---|---|
| `cdxt --cc --limit 10 iter 'example.com/*'` | `cdxt index cc --url 'example.com/*' --limit 10` |
| `cdxt --crawl 3 iter 'example.com/*'` | `cdxt index cc --url 'example.com/*' --crawl 3` |
| `cdxt --source URL iter 'example.com/*'` | `cdxt index URL --url 'example.com/*'` |
| `cdxt --cc size 'example.com/*'` | `cdxt index cc --url 'example.com/*' --count` |
| `cdxt --ia warc 'example.com/*'` | `cdxt index ia --url 'example.com/*' \| cdxt repackage --wb ia` |

All options now go after the command. `iter --csv`/`--jsonl` became
`--output-format`, all fields are written by default (`--all-fields` is
gone), `--get` is no longer needed (`--closest` uses a single query), and
`--url-fgrep`/`--url-fgrepv` moved from `warc` to `index`.


### The full syntax for command-line tools

```
$ cdxt --help
$ cdxt index --help
$ cdxt repackage --help
```

for full details. Add -v (or -vv) to see what's going on under the hood.

## Python programming example

Everything that you can do on the command line, and much more, can
be done by writing a Python program. `cdxt index` and `cdxt repackage`
run the same code as `open_index()` with an `IndexQuery`, and `repackage()`:

```
from cdx_toolkit.indexes import IndexQuery, open_index
from cdx_toolkit.repackage import repackage

index = open_index('cc')  # any INDEX of `cdxt index`, e.g. 'cc-url-index-s3' or 'athena://...'
query = IndexQuery(urls='commoncrawl.org/*', crawl='CC-MAIN-2024-33', limit=10)

print(index.count(query))    # like --count
print(index.explain(query))  # like --dry-run
for capture in index.records(query):
    print(capture['url'], capture['timestamp'])

result = repackage(index.records(query), prefix='out/COMMONCRAWL', size=100_000_000)
print(result.written, 'written,', result.failed, 'failed')
```

The fields of `IndexQuery` are the options of `cdxt index` (`urls`, `hostnames`, `domains`,
`url_file`, `crawl`, `from_ts`, `to`, `filters`, `languages`, `url_fgrep`, `limit`, `sql`, ...), and the
arguments of `repackage()` those of `cdxt repackage` (`subprefix`, `size`, `warc_download_prefix`,
`wb`, `processes`, `parallel_readers`, `metadata_records`, ...). `repackage()` also takes the path of
an index file. To write the captures to a file like `cdxt index -o`, use
`cdx_toolkit.record_io.write_records(index.records(query), 'captures.csv', 'csv', fixed_schema=index.fixed_schema)`.

`CDXFetcher` is the lower-level client of the CDX API, which can also
fetch the content of single captures:

```
import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc')
url = 'commoncrawl.org/*'

print(url, 'size estimate', cdx.get_size_estimate(url))

for obj in cdx.iter(url, limit=1):
    print(obj)
```

at the moment will print:

```
commoncrawl.org/* size estimate 36000
{'urlkey': 'org,commoncrawl)/', 'timestamp': '20180219112308', 'mime-detected': 'text/html', 'url': 'http://commoncrawl.org/', 'status': '200', 'filename': 'crawl-data/CC-MAIN-2018-09/segments/1518891812584.40/warc/CC-MAIN-20180219111908-20180219131908-00494.warc.gz', 'mime': 'text/html', 'length': '5365', 'digest': 'FM7M2JDBADOQIHKCSFKVTAML4FL2HPHT', 'offset': '81614902'}
```

You can also fetch the content of the web capture as bytes:

```
    print(obj.content)
```

There's a full example of selecting a subset of captures and writing them
into a WARC file in [examples/iter-and-warc.py](examples/iter-and-warc.py).
(`cdx_toolkit.warc.get_writer()` is deprecated: use `repackage()`.)


## CDX Jargon, Field Names, and such

cdx_toolkit supports all (ok, most!) of the options and fields discussed
in the CDX API documentation:

* https://github.com/webrecorder/pywb/wiki/CDX-Server-API
* https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server

A **capture** is a single crawled url, be it a copy of a webpage, a
redirect to another page, an error such as 404 (page not found), or a
revisit record (page identical to a previous capture.)

The **url** used by cdx_tools can be wildcarded in two ways. One way
is `*.example.com`, which in CDX jargon sets **matchType='domain'**,
and will return captures for example.com and blog.example.com and
support.example.com. The other, `example.com/*`, will return captures
for any page on example.com.

A **timestamp** represents year-month-day-time as a string of digits run togther.
Example: January 5, 2016 at 12:34:56 UTC is 20160105123456. These timestamps are
a field in the index, and are also used to pick specify the dates used
by **--from=**, **--to**, and **--closest** on the command-line. (Programmatically,
use **from_ts=**, to=, and closest=.)

An **urlkey** is a SURT, which is a munged-up url suitable for
deduplication and sorting. This sort order is how CDX indices
efficiently support queries like `*.example.com`. The SURTs for
www.example.com and example.com are identical, which is handy when
these 2 hosts actually have identical web content. The original url
should be present in all records, if you want to know exactly what it
is.

The **limit** argument limits how many captures will be returned.  To
help users not shoot themselves in the foot, a default limit of 1,000 is
applied to `CDXFetcher.get()` calls.

A **filter** allows a user to select a subset of CDX records, reducing
network traffic between the CDX API server and the user. For example,
filter='!status:200' will only show captures whose http status is not
200. Multiple filters can be specified as a list (in the api) and on
the command line (by specifying --filter more than once). Filters and
**limit** work together, with the limit applying to the count of
captures after the filter is applied. Note that revisit records have a
status of '-', not 200.

CDX API servers support a **paged interface** for efficient access to
large sets of URLs. cdx_toolkit iterators always use the paged interface.
cdx_toolkit is also polite to CDX servers by being single-threaded and
serial. If it's not fast enough for you, consider querying Common
Crawl's index files directly with `cdxt index` (CDX files or the columnar index).

A **digest** is a sha1 checksum of the contents of a capture. The
purpose of a digest is to be able to easily figure out if 2 captures
have identical content.

Common Crawl publishes a new index each month. cdx_toolkit will start
using new ones as soon as they are published. **By default,
cdx_toolkit will use the most recent 12 months of Common Crawl**; you
can change that using **--from** or **from_ts=** and **--to** or
**to=**.

CDX implementations do not efficiently support reversed sort orders,
so cdx_toolkit results will be ordered by ascending SURT and by
ascending timestamp. However, since CC has an individual index for
each month, and because most users want more recent results,
cdx_toolkit defaults to querying CC's CDX indices in decreasing month
order, but each month's result will be in ascending SURT and ascending
timestamp. This default sort order is named 'mixed'. If you'd like
pure ascending, set **--cc-sort** or **cc_sort=** to 'ascending'. You
may want to also specify **--from** or **from_ts=** to set a starting
timestamp.

The main problem with this ascending sort order is that it's a pain to
get the most recent N captures: --limit and limit= will return the
oldest N captures. With the 'mixed' ordering, a large enough limit=
will get close to returning the most recent N captures.

## TODO

Content downloading needs help with charset issues, preferably
figuring out the charset using an algorithm similar to browsers.

WARC generation should do smart(er) things with revisit records.

Without `--crawl`, the CC code selects which monthly CC indices to use
based solely on date ranges. It would be nice to have the default
one-year lookback use an entire monthly index instead of a partial one.

## Status

cdx_toolkit has reached the beta-testing stage of development.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing
and running tests.

## License

Copyright 2018-2020 Greg Lindahl and others

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
