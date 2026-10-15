# AWS: Athena and S3

Common Crawl's data is in the S3 bucket `s3://commoncrawl` in AWS `us-east-1`: the WARC files, the CDX
files and the [columnar URL index](https://commoncrawl.org/columnar-index). With an AWS account, you can
query the columnar index with [Athena](https://aws.amazon.com/athena/) (serverless SQL, billed per TB
scanned) or with DuckDB, and copy the selected records from S3. This is the fastest setup for large
selections, especially when `cdxt repackage` runs in `us-east-1` (e.g. on EC2), where reads from
`s3://commoncrawl` are fast and free of transfer costs.

You need:

- `pip install cdx_toolkit[athena,s3]` (and `[duckdb]` for DuckDB);
- AWS credentials, found like boto3 finds them (environment, `~/.aws` profiles, SSO, instance roles).
  Reads from `s3://commoncrawl` need credentials of any AWS account; anonymous requests are refused;
- for Athena: an S3 bucket of your own for the query results (`S3OutputLocation`), and the table of the
  columnar index in your account: run the `CREATE EXTERNAL TABLE` and `MSCK REPAIR TABLE` statements of
  [cc-index-table](https://github.com/commoncrawl/cc-index-table#query-the-table-in-amazon-athena) once
  (database `ccindex`, table `ccindex`), and repeat `MSCK REPAIR TABLE` to add new crawls.

The region, retries and timeouts of the S3, Athena and DuckDB requests are described in
[settings.md](../settings.md).

### Query URL Index via Athena

The Athena INDEX names the region, the database and the result location:

```sh
ATHENA='athena://athena.us-east-1.amazonaws.com/ccindex?S3OutputLocation=s3://my-bucket/athena-results/'
```

Add `&table=...` if your table is not called `ccindex`.

Send a query for a specific host name and a few crawls:

```sh
cdxt index "$ATHENA" --hostname commoncrawl.org --crawl CC-MAIN-2026-17,CC-MAIN-2026-13 -o captures.csv
```

`--dry-run` prints the SQL that `cdxt` generates, and `--count` runs a `count(*)` instead. The query is
restricted to the partitions of the given crawls and to `subset = 'warc'`; `--domain` and `--hostname` use
the column `url_host_name_reversed`, which follows the sort order of the index, so Athena skips most of
the data. With `-v`, `cdxt` logs the amount of data each query scanned and its estimated cost.

Get all records of a TLD for all crawls with your own SQL (`va.sql`):

```sql
SELECT url, fetch_time, fetch_status, content_mime_type, crawl,
       warc_filename, warc_record_offset, warc_record_length
FROM ccindex
WHERE subset = 'warc'
  AND url_host_tld = 'va'
```

```sh
cdxt index "$ATHENA" --sql va.sql -o va.csv
```

`--sql` runs the query as it is. Without a `crawl` condition, it scans the `url_host_tld` column of
every crawl, so `cdxt` asks for confirmation first (`--yes` skips the question). Try it on one crawl
first (`AND crawl = 'CC-MAIN-2026-17'`) to see the amount of data scanned. For `cdxt repackage`, a query
has to select `url`, `warc_filename`, `warc_record_offset` and `warc_record_length`.

Get the records of a list of hosts with a table join (like cc-index-table's
[count-domains-alexa-top-1m.sql](https://github.com/commoncrawl/cc-index-table/blob/main/src/sql/examples/cc-index/count-domains-alexa-top-1m.sql)).
Athena joins tables only, so upload the list to S3 and create a table for it once:

```sh
aws s3 cp hosts.csv s3://my-bucket/hosts/hosts.csv   # one host per line, e.g. www.commoncrawl.org

cat > create-hosts.sql <<'EOF'
CREATE EXTERNAL TABLE IF NOT EXISTS hosts (host string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://my-bucket/hosts/'
EOF
aws athena start-query-execution --query-string file://create-hosts.sql \
    --query-execution-context Database=ccindex \
    --result-configuration OutputLocation=s3://my-bucket/athena-results/
```

(or run the statement in the Athena console). Then join it with the index (`hosts.sql`):

```sql
SELECT cc.url, cc.fetch_time, cc.fetch_status, cc.crawl,
       cc.warc_filename, cc.warc_record_offset, cc.warc_record_length
FROM ccindex AS cc
JOIN hosts AS h ON cc.url_host_name = h.host
WHERE cc.crawl = 'CC-MAIN-2026-17'
  AND cc.subset = 'warc'
```

```sh
cdxt index "$ATHENA" --sql hosts.sql --yes -o hosts-captures.csv
```

### Repackage via S3 Bucket

Read the WARC records from `s3://commoncrawl` and write the WARC files to your bucket:

```sh
cdxt repackage hosts-captures.csv --warc-download-prefix s3://commoncrawl \
    --prefix s3://my-bucket/warc/HOSTS --size 1GB --processes 8 --parallel-readers 16
```

- `--warc-download-prefix s3://commoncrawl` reads with your credentials; in `us-east-1`, it is much
  faster than the default `https://data.commoncrawl.org`, which has a per-process rate limit.
- `--processes` (e.g. the number of vCPUs) and `--parallel-readers` (concurrent reads per process) set
  the throughput. The files are named `HOSTS-001.warc.gz`, `-002`, … whatever the number of processes.
- `--prefix` can also be a local path; `--add-metadata-record FILE` adds a file (e.g. the query) as a
  metadata record to each WARC file.

`cdxt index` can write the captures to S3 as well (`-o s3://my-bucket/hosts-captures.csv`), and
`cdxt repackage` can read them from there.

DuckDB works on the same data without Athena: `cdxt index cc-url-index-s3` queries
`s3://commoncrawl/cc-index/table/cc-main/warc/` with your credentials, with the same options, e.g.
`cdxt index cc-url-index-s3 --domain commoncrawl.org --crawl CC-MAIN-2026-17 -o captures.csv`. It costs
nothing but reads the Parquet footers of every file of the crawls, so it is fastest in `us-east-1`.
