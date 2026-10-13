# WARC fetcher performance: bottlenecks, analysis & recommendations

Notes on optimizing the `cdxt repackage` fetch pipeline for large jobs (e.g. a
range-jobs file with ~1M rows), with a focus on running on EC2 in-region against
`s3://commoncrawl`.

Status legend: **[done]** implemented on this branch · **[proposed]** not yet implemented.

> These notes were written on the `feat/warc-range-sources` branch (#73). In the new CLI the
> range-job sources became `cdxt index` (CDX API, Athena, DuckDB) and some options were renamed:
> `--parallel`/`--parallel_readers` → `--parallel-readers`, `--no-sort-ranges` → `--no-sort`,
> `--write-paths-as-metadata-records` → `--add-metadata-record`, `--confirm-cost` → `cdxt index --yes`.
> `--hf-reader` was removed: `hf://` is always read with the CDN reader, which now resolves each
> WARC file once instead of once per record.

---

## How the fetcher works today

Three async stages on a single `asyncio` event loop (`cdx_toolkit/repackage/repackager.py`):

1. **Produce** — the source (`RecordsSource`, the captures of the index file) yields `RangeJob`s (one per WARC record:
   url + byte offset + length). The source is a *sync* generator run in a worker
   thread (`asyncio.to_thread(drain)`, `repackager.py`), pushing each job onto a
   bounded `range_jobs_queue` via `run_coroutine_threadsafe(...).result()`.
2. **Read** — `num_readers` reader coroutines (`= --parallel`) each pull a job and do
   **one S3 `GetObject` (or HTTP GET) per job** (`read_warc_records` →
   `RangeReader.read`, `readers/`). The
   payload (raw, already-gzipped WARC record bytes) is enqueued.
3. **Write** — `num_writers` (`= num_readers/6`) writer coroutines each own one output
   shard and append records to it (`write_warc_records` → `WarcFileSeries.write`).

Clients/config:

- One shared `aioboto3` S3 read client and one write client (`get_aws_clients`,
  `repackager.py`); read pool sized `num_readers*3`.
- Retries/backoff for throttling (503 SlowDown) in `with_retries` (`retries.py`).
- Output writers: `S3OutputFile` (multipart upload, ≥5 MiB parts) or `LocalOutputFile`
  (`aiofiles`); shard files keyed by `writer_id` + `sequence`
  (`generate_warc_filename`, `writers/series.py`). Records are written **verbatim** — no
  recompression.

---

## Key insight: concurrency vs. cores

`aioboto3`/`asyncio` give **I/O concurrency**, not **multi-core parallelism**:

- With `--parallel=30` there are up to 30 S3 requests *in flight at once*; while most
  are blocked on the network, others run. For network-bound work this is exactly right.
- But asyncio is single-threaded: **one event loop, one OS thread, one CPU core.** All
  the *CPU* work — TLS encrypt/decrypt, HTTP framing, aiobotocore object churn, byte
  copies — is serialized on that one core. The readers **and** writers are all
  coroutines on the same loop (`asyncio.create_task(...)`, `repackager.py`).
- The GIL means *threads* wouldn't help CPU-bound work either. Only **multiple
  processes** use multiple cores.

Corollary: **sharded output ≠ multi-core.** Having N writers means N concurrent MPU
streams on one core, not N cores. Adding writers (or readers) past the point where the
single core saturates buys nothing but scheduling overhead and memory.

---

## What actually limits a 1M-row job in-region

**Bandwidth is rarely the constraint.** 1M records at ~30 KB compressed ≈ ~30 GB; over
even 5 Gbps that is ~50 s, at 25 Gbps ~10 s. A c5n NIC is nowhere near the bottleneck.

**Request rate on a single core usually is.** 1M independent range GETs, each with
per-request TLS + HTTP + Python overhead, all on one event-loop core. A single loop
typically sustains on the order of a few thousand small GET/s before that core
saturates → ~3–8 min CPU-bound on one core, with the NIC mostly idle and the box's other
vCPUs unused.

Two regimes, and the optimizations differ:

- **Many small requests** (default): single-core CPU/TLS bound → cut request count
  (coalescing), lift the single-core ceiling (uvloop), or use more cores
  (multi-process).
- **Fewer, larger reads** (after coalescing, or large records): network-bound → one core
  drives multiple Gbps fine; more processes buy little.

### How to tell which regime you're in

Run a sample and watch `htop`/`mpstat` during the fetch:

- One core pinned ~100 %, NIC well below capacity → **single-core CPU-bound** →
  coalescing / uvloop / multi-process.
- All cores low, throughput still short → **not CPU-bound**: the limiter is concurrency,
  request latency, or S3 throttling — process/writer count is irrelevant.

---

## Writer review (why more output shards won't fix a read-bound core)

- **No recompression** — records arrive already gzipped and are written verbatim
  (`writer.write(item.data)`, `repackager.py`). The writer adds no gzip CPU.
- **`S3OutputFile`** (`writers/s3.py`) buffers into a `bytearray`; on each ≥5 MiB
  boundary it does `buffer[:n]` (copy), `del buffer[:n]` (an O(remaining) memmove),
  `bytes(chunk)` (another copy), then `await upload_part`. Real but modest byte-copy cost.
- **`LocalOutputFile`** (`writers/local.py`) is `aiofiles` with an 8 KiB buffer and a
  `flush()` per write — on a fast read stream the per-flush overhead + EBS limits are the
  more likely bottleneck (prefer writing to `s3://` on a c5n).
- **[done] One writer per process.** Because writing is a cheap verbatim copy that one
  coroutine sustains, the old `num_writers = num_readers/6` multi-writer fan-out (multiple
  shards on one event loop) bought nothing but scheduling overhead and was removed. Each
  process now has exactly one writer = one output shard; multi-core scaling and sharding
  come from `--processes` (multiple event loops), and shards are merged into one file. The
  CPU hot spot is the **read side** (many small GETs + TLS), not the write side.

The flip side: the shard model makes **multi-process essentially free on the output
side** — shards are independent files with no cross-shard state and no merge step, so N
worker processes can each write their own shards into the same prefix.

---

## Recommendations (ranked)

Assume parallelism is already configured (`--parallel` high) and reads go to
`s3://commoncrawl` in-region.

1. **[done] Sort by `(warc_filename, warc_record_offset)`.** Groups records of the same
   WARC file with ascending offsets → read locality, connection reuse, and the
   prerequisite for coalescing and clean per-file process sharding. Implemented as an
   `ORDER BY` on guided SQL queries (Athena/DuckDB) and an in-memory sort when loading a
   CSV source; `--no-sort-ranges` opts out. Raw `--query` and `cdx` sources keep their
   order.

2. **[proposed] Coalesce adjacent ranges (highest remaining value).** After sorting,
   merge nearby same-file ranges into one GET (fetch the superrange, slice locally by
   offset). Safe because each WARC record is an independent gzip member, so concatenated
   members remain valid and byte-exact slicing reconstructs each record. Use a gap
   threshold (e.g. ≤256 KB–1 MB) to swallow the small request/metadata records between
   indexed responses. Effect: collapses ~1M tiny latency/CPU-bound GETs into ~10–100k
   larger sequential reads — directly attacks the request-rate ceiling; bandwidth easily
   absorbs the gap bytes. Extreme case: a **density heuristic** — when a large fraction of
   a ~1 GB file is needed, GET the whole file once and slice.

3. **[done] Use all cores via multi-process sharding** — confirmed single-core CPU-bound
   (a single loop saturates one core at ~450 small GET/s; one process pins ~1 core at
   `--parallel 96`). Implemented natively as `cdxt repackage --processes N`
   (`cdx_toolkit/repackage/multiprocess.py`): the source is drained once and sharded by
   `warc_filename`, one worker process per core fetches its shard
   (`--parallel_readers R`, one writer each → one shard/proc), and the shards are merged
   into a single `<prefix>.warc.gz` (`cdx_toolkit/repackage/merge.py`: server-side S3
   `UploadPartCopy`, or fsspec streaming locally). Measured on c5n.xlarge (4 vCPU):
   ~457 → ~1130 rec/s fetch (2.6×, cores ~97 %) for a ~1 GiB / 35.7 k-record homepages job;
   ~60 s end-to-end including source sharding and the merge. Sublinear because each
   process's producer thread (CSV sort + per-row cross-thread enqueue) competes with its
   event loop for the GIL — see latent issues.

4. **[done] uvloop.** Gated by `CDXT_UVLOOP=1` in `Repackager.run` (`repackager.py`).
   Measured ~+8 % single-core; stacks under multi-process.

5. **[proposed] Bound coalesced read size / watch RAM.** A c5n.xlarge has 10.5 GiB;
   whole-file (~1 GB) reads × many readers will OOM. Cap superrange size and gap
   threshold; revisit `warc_records_queue_size` (`repackager.py`, default 200) since
   each buffered payload is larger after coalescing.

6. **[done] Write output to `s3://` in-region, not local EBS.** Measured: S3-direct MPU
   output is within noise of local EBS at this write rate (~35 MB/s total) and skips the
   EBS round-trip + separate upload, so prefer `--prefix s3://…`. On the writer count: at
   in-region read rates a *single* writer per process keeps up easily (verbatim copy,
   5 MiB parts), so `--parallel_writers 1` is both fastest-enough and gives one shard per
   process — better than the `writers = readers/6` default for this workload.

7. **[proposed] Instance selection follows the constraint.** Because the binding limit is
   request-rate/CPU (not bandwidth), **more/faster cores beat more Gbps**. c5n.xlarge's
   "up to 25 Gbps" is burstable and depletes to baseline on long jobs anyway — but that
   barely matters when 30 GB is seconds of transfer. Scale vCPUs to feed (3); reserve
   sustained-25 Gbps types for genuinely bandwidth-bound (huge-record) jobs.

8. **[proposed] Dedup identical `(filename, offset, length)` rows** before fetching —
   free; fold into the sort/coalesce pass.

### Latent issues worth noting

- **HTTP path blocks the event loop.** The HTTP read (then in `RangeJob.ranged_get_bytes`) called the
  *synchronous* `myrequests_get(...)` without `await`/`to_thread`.
  Since the default `--warc-download-prefix` is `https://data.commoncrawl.org`, the
  default fetch path serializes the "parallel" readers. On EC2, prefer `s3://commoncrawl`
  (the S3 branch is properly async); otherwise the HTTP fetch should be offloaded to a
  thread pool / async HTTP client. **[done]**: `HttpRangeReader` (`readers/http.py`) runs it in a thread.
- **Per-row cross-thread enqueue.** The producer does one
  `run_coroutine_threadsafe(...).result()` per job (`repackager.py`) — 1M
  loop↔thread handoffs. Batching enqueues removes a real per-row overhead at this scale.
  **[proposed]**

---

## TL;DR

1. **[done]** Sort by `(warc_filename, offset)` — locality + enables coalescing.
2. **[done]** Multi-process by filename (`--processes N`) — ~2.6× on 4 cores; shards merged
   into one file. The big confirmed win in-region.
3. **[done]** uvloop (`CDXT_UVLOOP=1`), one writer per process, `s3://` output — supporting wins.
4. **[proposed]** Coalesce adjacent ranges, bounded read size, dedup — remaining ideas.

Always **measure the regime first** (`htop` during a sample run): in-region the limiter
is almost always request-rate on one core, not bandwidth.
