# Hugging Face

Recent Common Crawl crawls are also on Hugging Face, in the Storage Bucket
`hf://buckets/commoncrawl/commoncrawl` with the same paths as `s3://commoncrawl`: the WARC files and the
columnar URL index (`cc-index/table/cc-main/warc/`). See
[Getting started with Common Crawl data on Hugging Face](https://commoncrawl.org/blog/getting-started-with-common-crawl-data-on-hugging-face).
`cdxt` can query the index there with DuckDB, read the WARC records from the bucket, write the WARC
files to a bucket of your own, and run as a [Hugging Face Job](https://huggingface.co/docs/huggingface_hub/en/guides/jobs),
next to the data.

```sh
pip install cdx_toolkit[hf,duckdb]
hf auth login    # or set HF_TOKEN
```

Reads from the public bucket work without a token, but with lower
[rate limits](https://huggingface.co/docs/hub/en/rate-limits); writing to your bucket needs a token with
write access to it.

## Query URL index on Hugging Face via DuckDB

`cc-url-index-hf` takes the same options as `cc-url-index-s3`:

```sh
cdxt index cc-url-index-hf --hostname commoncrawl.org --crawl CC-MAIN-2026-17 -o captures.jsonl
```

This is slow: DuckDB reads the Parquet files through the Hub API, and each of a crawl's 300 files takes
several requests whose latency (and rate limits) add up to many minutes per crawl. Restrict the query
to as few crawls as possible, try it on a single file with `--sql` first:

```sql
SELECT url, fetch_time, fetch_status,
       warc_filename, warc_record_offset, warc_record_length
FROM read_parquet('hf://buckets/commoncrawl/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-17/subset=warc/part-00000-ee7620e8-27a7-4c74-846b-a92b039dae92.c000.gz.parquet')
WHERE url_host_tld = 'va'
```

```sh
cdxt index cc-url-index-hf --sql va.sql --yes -o va.csv
```

or run the query as an HF Job (below). For large queries, S3 is faster (see the
[AWS guide](aws-athena-s3.md)).

## Repackage via HF Bucket

```sh
cdxt repackage captures.jsonl --warc-download-prefix hf://buckets/commoncrawl/commoncrawl \
    --prefix hf://buckets/<you>/<bucket>/warc/CC --parallel-readers 8
```

`cdxt repackage` resolves each WARC file once and then reads its records from the signed download URL,
so the rate-limited Hub API sees one request per WARC file, not per record. `--prefix` can also be a
local path, and `cdxt index` can write to a bucket (`-o hf://buckets/<you>/<bucket>/captures.jsonl`).

## HF Jobs

An HF Job runs a command on Hugging Face's infrastructure, close to the bucket, without a machine of
your own (it needs a Hugging Face account with Jobs enabled, e.g. PRO or an organization with credits).
`hf jobs uv run` runs a Python script; this one (`cdxt_job.py`) installs `cdx_toolkit` and runs `cdxt`
with the arguments of the job:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = ["cdx_toolkit[hf,duckdb]"]
# ///
import sys

from cdx_toolkit.cli import main

main(args=sys.argv[1:])
```

Two jobs: the first selects the captures and writes them to your bucket, the second writes them to WARC
files in your bucket. `-s HF_TOKEN` passes your token (from the environment or `hf auth login`) to the
job, so it can write to the bucket.

1) `cdxt index`, writing to an HF bucket:

```sh
hf jobs uv run -s HF_TOKEN --timeout 2h cdxt_job.py \
    index cc-url-index-hf --hostname commoncrawl.org --crawl CC-MAIN-2026-17 \
    -o hf://buckets/<you>/<bucket>/captures.jsonl
```

2) `cdxt repackage`:

```sh
hf jobs uv run -s HF_TOKEN --flavor cpu-upgrade --timeout 2h cdxt_job.py \
    repackage hf://buckets/<you>/<bucket>/captures.jsonl \
    --warc-download-prefix hf://buckets/commoncrawl/commoncrawl \
    --prefix hf://buckets/<you>/<bucket>/warc/CC --parallel-readers 8
```

`hf jobs uv run` prints the job's logs until it finishes; with `--detach`, it prints the job ID and
returns, and `hf jobs logs <job-id>` and `hf jobs ps` follow it. `--timeout` sets the maximum duration,
`--flavor` the hardware (`hf jobs hardware` lists them), and `--namespace` runs the job in an
organization.
