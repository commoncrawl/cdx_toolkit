# HTTP

Everything in this guide works without an account or credentials: the indexes and the WARC files are
read over HTTPS from Common Crawl's and the Internet Archive's public servers.

- Common Crawl's CDX API (`index.commoncrawl.org`, one index per crawl) is described in
  [CDXJ index](https://commoncrawl.org/cdxj-index); the WARC files are on `https://data.commoncrawl.org`.
- The Internet Archive's CDX API and replays are described in the
  [Wayback Machine APIs](https://archive.org/help/wayback_api.php).

These servers are shared by everybody, so `cdxt` keeps a minimum interval between requests to each of
them (e.g. 1 s for `index.commoncrawl.org`, 6 s for `web.archive.org`) and retries when they ask it to
slow down; see [settings.md](../settings.md). That makes HTTP the right choice for small selections:
some hosts, URL prefixes or a few crawls. For large selections (a TLD, a long list of hosts), use the
columnar index (below) or, faster, [AWS](aws-athena-s3.md) or [Hugging Face](huggingface.md).

```sh
pip install cdx_toolkit           # CDX API and HTTP downloads
pip install cdx_toolkit[duckdb]   # also the columnar index with DuckDB
```

## Small scale

### Query CDX API Server

All captures of a host in the two most recent crawls (`--crawl 2`; or name them, e.g.
`--crawl CC-MAIN-2026-17,CC-MAIN-2026-13`):

```sh
cdxt index cc --hostname commoncrawl.org --crawl 2 -o captures.jsonl
```

Check the size of a selection first: `--count` is a cheap estimate based on the number of index pages,
and `--dry-run` prints the API requests without sending them:

```sh
cdxt index cc --hostname commoncrawl.org --crawl 2 --count
cdxt index cc --hostname commoncrawl.org --crawl 2 --dry-run
```

Only successful HTML captures, and only some fields (`--filter` uses the CDX API filter syntax, see the
README):

```sh
cdxt index cc --domain commoncrawl.org --crawl 2 \
    --filter =status:200 --filter '~mime:text/html' \
    --fields url,timestamp,status,mime -o captures.csv
```

`--url` takes the CDX API's URL patterns, e.g. `'commoncrawl.org/blog/*'` for a prefix or
`'*.commoncrawl.org'` for a domain.

The Internet Archive works the same way, with `--from`/`--to` instead of crawls:

```sh
cdxt index ia --url 'commoncrawl.org/*' --from 2012 --to 2013 --limit 100 -o ia-captures.jsonl
```

Write the captures to WARC files. Common Crawl captures are copied byte-for-byte from
`https://data.commoncrawl.org`; Internet Archive captures have no WARC location and are replayed from the
Wayback Machine with `--wb ia`:

```sh
cdxt repackage captures.jsonl --prefix out/COMMONCRAWL
cdxt repackage ia-captures.jsonl --wb ia --prefix out/IA
```

Both commands can also be piped:

```sh
cdxt index cc --hostname commoncrawl.org --crawl 1 --limit 20 | cdxt repackage --prefix out/CC-SAMPLE
```

## Large scale

### Query URL Index via DuckDB

The [columnar URL index](https://commoncrawl.org/columnar-index) has one row per capture with columns
such as `url_host_tld`, `url_host_registered_domain`, `content_languages` and the WARC location, in
Parquet files partitioned by `crawl` and `subset` (`warc`, `robotstxt`, `crawldiagnostics`). DuckDB
reads only the columns and row groups a query needs.

`cdxt index cc-url-index-s3` reads it from `s3://commoncrawl`, which needs AWS credentials (see the
[AWS guide](aws-athena-s3.md)). Without credentials, DuckDB reads the same files over HTTPS. HTTPS can't
list a directory, so the query names the files; every crawl has a list of them in
`crawl-data/<crawl>/cc-index-table.paths.gz`:

```sh
CRAWL=CC-MAIN-2026-17
curl -s https://data.commoncrawl.org/crawl-data/$CRAWL/cc-index-table.paths.gz | gunzip \
    | grep '/subset=warc/' | sed "s|^|'https://data.commoncrawl.org/|; s|\$|'|" | paste -sd, - > files.txt
```

All captures of a TLD (here `.va`) in that crawl, in `tld.sql`:

```sh
cat > tld.sql <<EOF
SELECT url, fetch_time, fetch_status, content_mime_type,
       warc_filename, warc_record_offset, warc_record_length
FROM read_parquet([$(cat files.txt)])
WHERE url_host_tld = 'va'
EOF
cdxt index cc-url-index-s3 --sql tld.sql --yes -o va.csv
```

`--sql` runs the query as it is (`--dry-run` prints it), so INDEX only selects DuckDB. For
`cdxt repackage`, a query has to select `url`, `warc_filename`, `warc_record_offset` and
`warc_record_length`. For a few crawls, concatenate their file lists.

All captures of a list of hosts: join the index with a CSV file of hosts (`hosts.csv` with a `host`
column, e.g. `www.commoncrawl.org`):

```sh
cat > hosts.sql <<EOF
SELECT cc.url, cc.fetch_time, cc.fetch_status,
       cc.warc_filename, cc.warc_record_offset, cc.warc_record_length
FROM read_parquet([$(cat files.txt)]) AS cc
JOIN read_csv('hosts.csv') AS h ON cc.url_host_name = h.host
EOF
cdxt index cc-url-index-s3 --sql hosts.sql --yes -o hosts-captures.csv
```

Each of these queries reads the selected columns of all files of a crawl (300 files) over HTTPS, which
takes a while; restrict the columns and crawls as far as possible.

### Repackage via CC's HTTP Server

`cdxt repackage` downloads from `https://data.commoncrawl.org` by default:

```sh
cdxt repackage va.csv --prefix out/VA --size 100MB --parallel-readers 4
```

Requests to `data.commoncrawl.org` are at least 0.55 s apart per process, whatever
`--parallel-readers` is, so a process reads at most about 1.8 records per second (see
[settings.md](../settings.md)). `--processes N` multiplies that; please keep N small, the server is
shared. For millions of records, read from `s3://commoncrawl` in AWS us-east-1 instead (see the
[AWS guide](aws-athena-s3.md)).

The WARC files are named `<prefix>-001.warc.gz`, `-002`, …, each about `--size` (default 1 GB), with a
`warcinfo` record that lists the command line.
