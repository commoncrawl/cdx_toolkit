import asyncio
import concurrent.futures
import contextlib
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple


from cdx_toolkit.aws import botocore_config
from cdx_toolkit.repackage.jobs import RangePayload, RepackageResult
from cdx_toolkit.repackage.readers import Readers
from cdx_toolkit.repackage.readers.http import run_in_daemon_thread
from cdx_toolkit.repackage.urls import is_s3_url
from cdx_toolkit.repackage.writers.series import WarcFileSeries


_STOP = object()

logger = logging.getLogger(__name__)

# the name of the thread that reads the source (see Repackager._produce_range_jobs and source_still_reading)
SOURCE_THREAD_NAME = 'cdxt-source'


def source_threads() -> set:
    """The source threads that are running: reading their input, maybe blocked on it (e.g. on stdin from a
    process that keeps running) after their pipeline stopped."""
    return {t for t in threading.enumerate() if t.name == SOURCE_THREAD_NAME and t.is_alive()}


@dataclass
class ThroughputTracker:
    """Items (range reads, or written records), bytes and WARC records of a reader or the writer, for the logs."""

    start_time: float = field(default_factory=time.time)
    total_bytes: int = 0
    total_requests: int = 0
    total_records: int = 0

    def add(self, bytes_count: int = 0, records_count: int = 0, requests_count: int = 1):
        self.total_bytes += bytes_count
        self.total_requests += requests_count
        self.total_records += records_count

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time

        return {
            'elapsed': elapsed,
            'total_bytes': self.total_bytes,
            'total_requests': self.total_requests,
            'total_records': self.total_records,
            'bytes_per_sec': self.total_bytes / elapsed if elapsed > 0 else 0,
            'mb_per_sec': (self.total_bytes / elapsed) / (1024 * 1024) if elapsed > 0 else 0,
            'requests_per_sec': self.total_requests / elapsed if elapsed > 0 else 0,
            'records_per_sec': self.total_records / elapsed if elapsed > 0 else 0,
        }

    def summary(self) -> str:
        stats = self.get_stats()
        return (
            f"{stats['total_requests']} items, {stats['total_bytes'] / (1024 * 1024):.1f} MB "
            f"in {stats['elapsed']:.1f}s, {stats['mb_per_sec']:.2f} MB/s, {stats['records_per_sec']:.2f} rec/s"
        )


class RepackageCancelled(Exception):
    """The pipeline was stopped from outside (its `cancel` event was set), e.g. because another process failed."""


class Repackager:
    """Copy WARC records, byte for byte, from the WARC files of the range jobs (or the records replayed from a
    wayback machine) into a series of WARC files.

    A pipeline of asyncio tasks:
    - the producer drives the (sync) source in a thread and puts its range jobs into a queue,
    - N reader tasks read the bytes of the jobs, with the RangeReader of each job
      (HTTP, S3, Hugging Face buckets by the URL scheme, or the wayback reader; see readers/),
    - one writer task writes them to the WarcFileSeries (local files or S3 objects; see writers/).

    Multi-core scaling is done with processes (see multiprocess.py), each with its own pipeline.
    """

    def __init__(
        self,
        prefix_path: str,
        writer_info: Dict,
        source,
        writer_subprefix: Optional[str] = None,
        write_paths_as_metadata_records: Optional[List[str]] = None,
        log_every_n: int = 1000,
        n_parallel_readers: int = 1,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
        range_jobs_queue_size: int = 1000,
        warc_records_queue_size: int = 200,
        aws_region_name: Optional[str] = None,
        warc_version: str = '1.0',
        min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
        max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
        next_file_sequence: Optional[Callable[[int], int]] = None,
        cancel=None,
    ):
        """Set up the pipeline.

        Args:
            source: The WARC ranges to repackage: an object with iter_range_jobs() (e.g. a RecordsSource).
            prefix_path: Output path prefix for the WARC files.
            writer_info: Dictionary containing writer metadata.
            writer_subprefix: Optional subprefix for writer output paths.
            write_paths_as_metadata_records: Optional list of file paths to write as metadata records.
            log_every_n: Log progress every N records.
            n_parallel_readers: Number of async readers per process (one writer per process).
            max_attempts: Maximum retry attempts for failed operations.
            base_backoff_seconds: Base backoff time in seconds for retries.
            range_jobs_queue_size: Maximum size of range jobs queue.
            warc_records_queue_size: Maximum size of WARC records queue.
            aws_region_name: AWS region for S3 operations (default: the user's AWS configuration, else us-east-1).
            warc_version: WARC format version (e.g., '1.0' or '1.1').
            min_part_size: Minimum part byte size for multipart uploads (default: 5 MiB).
            max_file_size: Maximum byte size for individual WARC files (default: 1 GiB).
            next_file_sequence: Number of the next output file, given the current one
                (default: current + 1).
            cancel: Optional event (threading.Event, multiprocessing.Event): once it is set, the pipeline stops
                like on a failure (the current file is aborted) and run() raises RepackageCancelled.
        """
        if n_parallel_readers < 1:
            raise ValueError(f'n_parallel_readers must be at least 1, not {n_parallel_readers}')
        self.source = source
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.write_paths_as_metadata_records = write_paths_as_metadata_records
        self.log_every_n = log_every_n

        self.range_jobs_queue_size = range_jobs_queue_size
        self.warc_records_queue_size = warc_records_queue_size
        self.aws_region_name = aws_region_name
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds

        # Many async readers feed a single writer. Writing is a cheap verbatim byte copy
        # (+ >=5 MiB MPU parts) that one coroutine sustains easily; multi-core scaling and
        # output sharding are handled by running multiple *processes* (see
        # cdx_toolkit/repackage/multiprocess.py), not multiple writers on one event loop.
        self.num_readers = n_parallel_readers

        self.gzip = True

        self.warc_version = warc_version
        self.min_part_size = min_part_size
        self.max_file_size = max_file_size
        self.next_file_sequence = next_file_sequence  # see WarcFileSeries
        self.cancel = cancel

    def run(self) -> RepackageResult:
        """Run the pipeline (runs run_async in an event loop).

        Captures that cannot be fetched are counted as failed (the others are still written); a failure to
        write raises.
        """
        runner = asyncio.run
        if os.environ.get('CDXT_UVLOOP') == '1':
            try:
                import uvloop

                runner = uvloop.run
                logger.info('Using uvloop event loop')
            except ImportError:
                logger.warning('CDXT_UVLOOP=1 but uvloop is not installed; using default asyncio loop')
        try:
            return runner(self._run_with_sigint())
        except asyncio.CancelledError:
            if self.cancel is not None and self.cancel.is_set():
                raise RepackageCancelled('stopped (cancel event)') from None
            raise

    async def _run_with_sigint(self) -> RepackageResult:
        """run_async(); before Python 3.11, Ctrl-C (SIGINT) cancels it like asyncio.run() does since 3.11, so that it
        stops at an await (and aborts the current file) instead of wherever KeyboardInterrupt is raised."""
        if sys.version_info >= (3, 11):
            return await self.run_async()
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        interrupted = []

        def on_sigint():
            interrupted.append(True)
            task.cancel()

        try:
            loop.add_signal_handler(signal.SIGINT, on_sigint)
        except (NotImplementedError, RuntimeError, ValueError):  # Windows, or not the main thread
            return await self.run_async()
        try:
            return await self.run_async()
        except asyncio.CancelledError:
            if interrupted:
                raise KeyboardInterrupt from None
            raise
        finally:
            loop.remove_signal_handler(signal.SIGINT)

    async def _watch_cancel(self, pipeline: asyncio.Task) -> None:
        """Cancel the pipeline once the cancel event is set."""
        while not self.cancel.is_set():
            await asyncio.sleep(0.1)
        logger.warning('Stopping: cancelled from outside')
        pipeline.cancel()

    def s3_read_config(self):
        """botocore config of the S3 client for range reads: high throughput."""
        # region, retries and S3 settings: Common Crawl's recommended defaults, unless the
        # user configured them (see cdx_toolkit.aws); an explicit aws_region_name wins
        return botocore_config(
            region_name=self.aws_region_name,
            max_pool_connections=self.num_readers * 3,
            read_timeout=300,
            tcp_keepalive=True,
        )

    def s3_write_config(self):
        """botocore config of the S3 client for multipart uploads (single writer)."""
        return botocore_config(
            region_name=self.aws_region_name,
            max_pool_connections=8,
            read_timeout=120,
            connect_timeout=10,
        )

    async def run_async(self) -> RepackageResult:
        """Run the three stages: producer of range jobs, reader tasks, writer task."""
        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        async with contextlib.AsyncExitStack() as stack:
            write_s3_client = None
            if is_s3_url(self.prefix_path):
                import aioboto3

                client = aioboto3.Session().client('s3', config=self.s3_write_config())
                write_s3_client = await stack.enter_async_context(client)

            return await self._run_pipeline(
                range_jobs_queue=range_jobs_queue,
                warc_records_queue=warc_records_queue,
                readers=Readers(stack, self.s3_read_config, self.max_attempts, self.base_backoff_seconds),
                write_s3_client=write_s3_client,
            )

    async def _produce_range_jobs(self, range_jobs_queue: asyncio.Queue, stop: threading.Event) -> int:
        """Drive the (sync) source in a daemon thread, feeding the async queue.

        The thread waits for queue space in short steps, so it stops soon after `stop` is set (cancellation,
        or a failed writer), also when the queue is full and nobody reads from it anymore. A source that blocks
        (e.g. stdin from a process that is still running) cannot be interrupted: nothing waits for its thread,
        not this task when it is cancelled, not asyncio.run() and not the interpreter at exit."""
        loop = asyncio.get_running_loop()

        def drain() -> int:
            threading.current_thread().name = SOURCE_THREAD_NAME
            count = 0
            for job in self.source.iter_range_jobs():
                if stop.is_set():
                    return count
                try:
                    future = asyncio.run_coroutine_threadsafe(range_jobs_queue.put(job), loop)
                except RuntimeError:  # the event loop is closed
                    return count
                while True:
                    if stop.is_set():
                        future.cancel()
                        return count
                    try:
                        future.result(timeout=0.1)
                        break
                    except concurrent.futures.TimeoutError:
                        pass
                count += 1
            return count

        try:
            count = await run_in_daemon_thread(drain)
        finally:
            stop.set()  # the thread keeps running if this task is cancelled; let it stop

        logger.info('Generated %d range jobs', count)
        return count

    async def _run_pipeline(
        self,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        readers: Readers,
        write_s3_client=None,
    ) -> RepackageResult:
        """Run the pipeline, with or without an S3 client for the output.

        A failing writer (or source) stops the whole pipeline, as does cancellation: all tasks are cancelled,
        and the writer aborts the file it is writing (no incomplete WARC file or S3 object is left).

        Args:
            range_jobs_queue: Queue for range jobs from the source.
            warc_records_queue: Queue for WARC record payloads.
            readers: The readers of the jobs (see Readers).
            write_s3_client: Optional S3 client for writes S3.
        """
        logger.info('Starting job generator, %d WARC readers, 1 WARC writer', self.num_readers)
        stop = threading.Event()

        # Generate range jobs from the configured source (bridged sync->async in a thread).
        producer = asyncio.create_task(self._produce_range_jobs(range_jobs_queue, stop))

        # Read WARC records based on file paths and ranges
        warc_readers = [
            asyncio.create_task(
                self.read_warc_records(
                    reader_id=i,
                    range_jobs_queue=range_jobs_queue,
                    warc_records_queue=warc_records_queue,
                    readers=readers,
                )
            )
            for i in range(self.num_readers)
        ]

        # Write WARC records (a single writer owns the output shard for this process)
        warc_writer = asyncio.create_task(
            self.write_warc_records(
                warc_records_queue=warc_records_queue,
                s3_client=write_s3_client,
            )
        )

        async def feed():
            """Wait for the jobs to be produced and read, then stop the readers and the writer."""
            requested = await producer
            for _ in warc_readers:
                await range_jobs_queue.put(_STOP)
            results = await asyncio.gather(*warc_readers)
            await warc_records_queue.put(_STOP)
            return requested, results

        feeder = asyncio.create_task(feed())
        tasks = [producer, *warc_readers, warc_writer, feeder]
        watcher = None
        if self.cancel is not None:
            watcher = asyncio.create_task(self._watch_cancel(asyncio.current_task()))
            tasks.append(watcher)
        try:
            done, _ = await asyncio.wait({feeder, warc_writer}, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()  # raises the exception of a failed task
            requested, readers_results = await feeder
            writer_tracker = await warc_writer
            if watcher is not None:
                watcher.cancel()
        except BaseException:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        reader_stats = [tracker.get_stats() for tracker, _ in readers_results]
        logger.info(
            'All WARC readers completed: %d records, %.2f MB/s, %.2f rec/s',
            sum(stats['total_records'] for stats in reader_stats),
            sum(stats['mb_per_sec'] for stats in reader_stats),
            sum(stats['records_per_sec'] for stats in reader_stats),
        )
        logger.info('WARC writer completed: %s', writer_tracker.summary())

        failed = sum(failed for _, failed in readers_results)
        if failed:
            logger.error('%d of %d captures could not be fetched', failed, requested)
        return RepackageResult(requested=requested, written=writer_tracker.total_records, failed=failed)

    async def read_warc_records(
        self,
        reader_id: int,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        readers: Readers,
    ) -> Tuple[ThroughputTracker, int]:
        """Read WARC records based on range jobs -> enqueue RangePayload.

        Args:
            reader_id: Unique identifier for this reader task.
            range_jobs_queue: Queue to read the jobs (RangeJob, WaybackJob) from.
            warc_records_queue: Queue to put RangePayload objects into.
            readers: The readers of the jobs (see Readers).

        Returns:
            The reader's throughput and the number of range jobs that could not be read.
        """
        tracker = ThroughputTracker()
        failed = 0

        while True:
            job = await range_jobs_queue.get()
            if job is _STOP:
                logger.info('WARC Reader %d stopping: %s', reader_id, tracker.summary())
                return tracker, failed
            try:
                reader = await readers.for_job(job)
                data = await reader.read(job)
            except Exception as e:
                # the capture is lost, the others are still fetched; the run fails at the end
                failed += 1
                logger.error('WARC Reader %d failed on %s: %r', reader_id, job, e)
                continue
            tracker.add(bytes_count=len(data), records_count=job.records_count)
            self.log_progress(f'WARC Reader {reader_id}', tracker)
            await warc_records_queue.put(RangePayload(job=job, data=data))

    async def write_warc_records(
        self,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ) -> ThroughputTracker:
        """Write the WARC records to the series of output files (one series per process), rotating at --size.

        Args:
            warc_records_queue: Queue to read RangePayload objects from.
            s3_client: Optional S3 client for writing WARC files to S3.

        Returns:
            The writer's throughput.
        """
        series = WarcFileSeries(
            self.prefix_path,
            self.writer_info,
            writer_subprefix=self.writer_subprefix,
            metadata_paths=self.write_paths_as_metadata_records,
            warc_version=self.warc_version,
            gzip=self.gzip,
            max_file_size=self.max_file_size,
            next_file_sequence=self.next_file_sequence,
            s3_client=s3_client,
            min_part_size=self.min_part_size,
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
        )
        tracker = ThroughputTracker()

        # any failure is fatal, also when closing a file: the current file is aborted (removed, or its S3 upload
        # aborted), the files completed before stay
        try:
            while True:
                item = await warc_records_queue.get()
                if item is _STOP:
                    break
                assert isinstance(item, RangePayload)
                await series.write(item.data)
                tracker.add(bytes_count=len(item.data), records_count=item.job.records_count)
                self.log_progress('WARC Writer', tracker)
            await series.close()
        except asyncio.CancelledError:
            # stopped by the pipeline: another task (e.g. the source) failed, or the run was cancelled
            logger.info('WARC writer stopped, aborting the current WARC file')
            await series.abort()
            raise
        except BaseException:
            logger.error('WARC writer failed, aborting the current WARC file')
            await series.abort()
            raise
        return tracker

    def log_progress(self, name: str, tracker: ThroughputTracker) -> None:
        """Log the progress of a reader or the writer every --log-every-n items."""
        if self.log_every_n > 0 and tracker.total_requests % self.log_every_n == 0:
            logger.info('%s: %s', name, tracker.summary())
