import asyncio
import concurrent.futures
import contextlib
import logging
import os
import statistics
import threading
from typing import Callable, List, Optional, Dict


from cdx_toolkit.aws import botocore_config
from cdx_toolkit.repackage.s3_utils import (
    is_s3_url,
)
from cdx_toolkit.repackage.hf_utils import make_hf_reader
from cdx_toolkit.repackage.data_classes import RangeJob, RangePayload, RepackageResult, ThroughputTracker
from cdx_toolkit.repackage.warc_utils import create_new_writer_with_header
from cdx_toolkit.repackage.warc_utils import get_bytes_from_warc_record, get_metadata_record_from_path


_STOP = object()

logger = logging.getLogger(__name__)


class Readers:
    """The readers of WARC ranges, by the URL scheme of a range job, each created when it is first needed:
    an S3 client for s3:// and the HF reader for hf:// (http(s):// needs none). They are closed with `stack`."""

    def __init__(self, stack: contextlib.AsyncExitStack, s3_config_factory: Callable):
        self.stack = stack
        self.s3_config_factory = s3_config_factory
        self._s3 = None
        self._hf = None
        self._lock = asyncio.Lock()

    async def s3(self):
        async with self._lock:
            if self._s3 is None:
                import aioboto3

                client = aioboto3.Session().client('s3', config=self.s3_config_factory())
                self._s3 = await self.stack.enter_async_context(client)
            return self._s3

    async def hf(self):
        async with self._lock:
            if self._hf is None:
                self._hf = await self.stack.enter_async_context(make_hf_reader())
            return self._hf


class WARCFilter:
    """Filter or extract specific records from WARC files based on CDX indexes.

    The WARC filter uses a three stage listner-producer-consumer pattern.

    Filter targets:
    - CDX index files from local or remote file system, containing paths to WARC files and positions of target records.

    WARC reader:
    - HTTP range reads
    - S3 range reads

    WARC writer:
    - Local file system
    - S3 using multi-part uploads
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
        # writer_kwargs: Optional[Dict] = None,
        range_jobs_queue_size: int = 1000,
        warc_records_queue_size: int = 200,
        aws_region_name: Optional[str] = None,
        warc_version: str = '1.0',
        content_type: Optional[str] = None,
        min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
        max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
        next_file_sequence: Optional[Callable[[int], int]] = None,
    ):
        """Initialize the WARC filter.

        Args:
            source: The WARC ranges to repackage: an object with iter_range_jobs() (e.g. a RecordsSource).
            prefix_path: Output path prefix for filtered WARC files.
            writer_info: Dictionary containing writer metadata.
            writer_subprefix: Optional subprefix for writer output paths.
            write_paths_as_metadata_records: Optional list of file paths to write as metadata records.
            log_every_n: Log progress every N records.
            n_parallel_readers: Number of async readers per process (one writer per process).
            max_attempts: Maximum retry attempts for failed operations.
            base_backoff_seconds: Base backoff time in seconds for retries.
            writer_kwargs: Optional additional kwargs for writers.
            range_jobs_queue_size: Maximum size of range jobs queue.
            warc_records_queue_size: Maximum size of WARC records queue.
            aws_region_name: AWS region for S3 operations (default: the user's AWS configuration, else us-east-1).
            warc_version: WARC format version (e.g., '1.0' or '1.1').
            content_type: Optional content type for WARC output.
            min_part_size: Minimum part byte size for multipart uploads (default: 5 MiB).
            max_file_size: Maximum byte size for individual WARC files (default: 1 GiB).
            next_file_sequence: Number of the next output file, given the current one
                (default: current + 1).
        """
        if n_parallel_readers < 1:
            raise ValueError(f'n_parallel_readers must be at least 1, not {n_parallel_readers}')
        self.source = source
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.write_paths_as_metadata_records = write_paths_as_metadata_records
        self.log_every_n = log_every_n

        # self.writer_kwargs = writer_kwargs
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
        self.content_type = content_type
        self.min_part_size = min_part_size
        self.max_file_size = max_file_size
        # Number of the next output file, given the current one (0 before the first file). The
        # multi-process mode shares one counter across processes, so file names are final right away.
        self.next_file_sequence = next_file_sequence or (lambda sequence: sequence + 1)

    def filter(self) -> RepackageResult:
        """Perform the filtering process (calls async method via asyncio.run).

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
        return runner(self.filter_async())

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

    async def filter_async(self) -> RepackageResult:
        """Filter process using a three stage approach (job generator, warc reader, warc writer)."""
        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        async with contextlib.AsyncExitStack() as stack:
            write_s3_client = None
            if is_s3_url(self.prefix_path):
                import aioboto3

                client = aioboto3.Session().client('s3', config=self.s3_write_config())
                write_s3_client = await stack.enter_async_context(client)

            return await self._run_filter_pipeline(
                range_jobs_queue=range_jobs_queue,
                warc_records_queue=warc_records_queue,
                readers=Readers(stack, self.s3_read_config),
                write_s3_client=write_s3_client,
            )

    async def _produce_range_jobs(self, range_jobs_queue: asyncio.Queue, stop: threading.Event) -> int:
        """Drive the (sync) source in a worker thread, feeding the async queue.

        The thread waits for queue space in short steps, so it stops soon after `stop` is set (cancellation,
        or a failed writer), also when the queue is full and nobody reads from it anymore."""
        loop = asyncio.get_running_loop()

        def drain() -> int:
            count = 0
            for job in self.source.iter_range_jobs():
                future = asyncio.run_coroutine_threadsafe(range_jobs_queue.put(job), loop)
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
            count = await asyncio.to_thread(drain)
        finally:
            stop.set()  # the thread keeps running if this task is cancelled; let it stop

        logger.info('Generated %d range jobs', count)
        return count

    async def _run_filter_pipeline(
        self,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        readers: Readers,
        write_s3_client=None,
    ) -> RepackageResult:
        """Run the actual filter pipeline with or without S3 client.

        A failing writer (or source) stops the whole pipeline, as does cancellation: all tasks are cancelled,
        and the writer aborts the file it is writing (no incomplete WARC file or S3 object is left).

        Args:
            range_jobs_queue: Queue for range jobs from the source.
            warc_records_queue: Queue for WARC record payloads.
            readers: The readers of WARC ranges (by URL scheme).
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
        try:
            done, _ = await asyncio.wait({feeder, warc_writer}, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()  # raises the exception of a failed task
            requested, readers_results = await feeder
            writer_result = await warc_writer
        except BaseException:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        readers_records = sum([result['stats']['total_records'] for result in readers_results])
        readers_mb_per_sec = self.num_readers * statistics.mean(
            [result['stats']['mb_per_sec'] for result in readers_results]
        )
        readers_records_per_sec = self.num_readers * statistics.mean(
            [result['stats']['records_per_sec'] for result in readers_results]
        )

        logger.info(f'All WARC readers completed: {readers_records} records')
        logger.info(f'Total reader throughput: {readers_mb_per_sec:.2f} MB/s; {readers_records_per_sec:.2f} rec/s')

        writer_stats = writer_result['stats']
        logger.info(f"WARC writer completed: {writer_stats['total_records']} records")
        logger.info(
            f"Total writer throughput: {writer_stats['mb_per_sec']:.2f} MB/s; "
            f"{writer_stats['records_per_sec']:.2f} rec/s"
        )

        failed = sum(result['failed'] for result in readers_results)
        if failed:
            logger.error('%d of %d captures could not be fetched', failed, requested)
        return RepackageResult(requested=requested, written=writer_stats['total_records'], failed=failed)

    async def read_warc_records(
        self,
        reader_id: int,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        readers: Readers,
    ) -> dict:
        """Read WARC records based on range jobs -> enqueue RangePayload.

        Args:
            reader_id: Unique identifier for this reader task.
            range_jobs_queue: Queue to read RangeJob objects from.
            warc_records_queue: Queue to put RangePayload objects into.
            readers: The readers of WARC ranges (by URL scheme).

        Returns:
            dict: Statistics dictionary with reader_id and throughput stats.
        """
        tracker = ThroughputTracker()
        tracker.start()
        counter = 0
        failed = 0

        while True:
            job = await range_jobs_queue.get()
            try:
                if job is _STOP:
                    stats = tracker.get_stats()
                    logger.info(
                        'WARC Reader %d stopping. Stats: %.1fs, %d requests, %.1f MB, %.2f MB/s, %.2f req/s',
                        reader_id,
                        stats['elapsed'],
                        stats['total_requests'],
                        stats['total_bytes'] / (1024 * 1024),
                        stats['mb_per_sec'],
                        stats['requests_per_sec'],
                    )
                    break  # Exit loop, but still execute finally block
                assert isinstance(job, RangeJob)
                data = await job.ranged_get_bytes(
                    self.max_attempts,
                    self.base_backoff_seconds,
                    readers=readers,
                )
                tracker.add(bytes_count=len(data), records_count=job.records_count)
                counter += 1

                # Log progress every N items
                self.log_reader(reader_id=reader_id, counter=counter, tracker=tracker)

                await warc_records_queue.put(RangePayload(job=job, data=data))
            except Exception as e:
                # the capture is lost, the others are still fetched; the run fails at the end
                failed += 1
                logger.error('WARC Reader %d failed on %s [%d,%d]: %r', reader_id, job.url, job.offset, job.length, e)

        return {'reader_id': reader_id, 'stats': tracker.get_stats(), 'failed': failed}

    async def write_metadata_records(self, writer, warcinfo_id: str) -> int:
        """Write WARC metadata records based on paths"""
        records_size = 0
        records_count = 0

        logger.info(f'Writing {len(self.write_paths_as_metadata_records)} metadata records to WARC ... ')

        # Metadata records are written at the beginning each WARC file.
        for i, resource_record_path in enumerate(self.write_paths_as_metadata_records):
            logger.info(f'Writing metadata record from {resource_record_path} ...')
            record = get_metadata_record_from_path(
                file_path=resource_record_path,
                warcinfo_id=warcinfo_id,
            )
            record_data = get_bytes_from_warc_record(record, warc_version=self.warc_version, gzip=self.gzip)
            await writer.write(record_data)

            # Keep track but do not rotate resource records
            records_size += len(record_data)
            records_count += 1

        logger.info(f'Metadata records added: {records_count}')

        return records_size

    async def write_warc_records(
        self,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
    ) -> dict:
        """Write WARC records. The single writer owns the output WARC (one shard per
        process) and appends ranges to it, rotating by --size when set.

        Args:
            warc_records_queue: Queue to read RangePayload objects from.
            s3_client: Optional S3 client for writing WARC files to S3.

        Returns:
            dict: Statistics dictionary with throughput stats.
        """
        # File rotation tracking; the first file is only created with the first record, so
        # no WARC file is written without records
        writer = None
        current_file_sequence = 0
        current_file_size = 0
        records_in_file = 0

        new_writer_kwargs = dict(
            s3_client=s3_client,
            output_path_prefix=self.prefix_path,
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
            writer_info=self.writer_info,
            warc_version=self.warc_version,
            writer_subprefix=self.writer_subprefix,
            gzip=self.gzip,
            content_type=self.content_type,
            min_part_size=self.min_part_size,
        )

        tracker = ThroughputTracker()
        tracker.start()
        counter = 0

        # Response records; any failure is fatal, also when closing a file: the current file is aborted
        # (removed, or its S3 upload aborted), the files completed before stay
        try:
            while True:
                item = await warc_records_queue.get()
                if item is _STOP:
                    stats = tracker.get_stats()
                    logger.info(
                        'WARC writer stopping. Stats: %.1fs, %d items, %.1f MB written, %.2f MB/s write speed',
                        stats['elapsed'],
                        stats['total_requests'],
                        stats['total_bytes'] / (1024 * 1024),
                        stats['mb_per_sec'],
                    )
                    break
                assert isinstance(item, RangePayload)
                counter += 1

                if writer is None:
                    # first file, with header and metadata records
                    current_file_sequence = self.next_file_sequence(0)
                    writer, current_file_size = await self.start_file(current_file_sequence, **new_writer_kwargs)
                    records_in_file = 0
                elif records_in_file and self.max_file_size and \
                        current_file_size + len(item.data) > self.max_file_size:
                    # rotate due to the size limit (a file gets at least one record, also if the record is
                    # larger than the size limit); the finished file is not aborted if starting the next fails
                    await writer.close()
                    writer = None
                    current_file_sequence = self.next_file_sequence(current_file_sequence)
                    writer, current_file_size = await self.start_file(current_file_sequence, **new_writer_kwargs)
                    records_in_file = 0
                    logger.info(f'Rotated to new WARC file sequence {current_file_sequence} due to size limit')

                # Write actual response record
                await writer.write(item.data)
                records_in_file += 1
                current_file_size += len(item.data)
                tracker.add(bytes_count=len(item.data), records_count=item.job.records_count)

                # Log progress every N items
                self.log_writer(counter=counter, tracker=tracker)

            if writer is not None:
                await writer.close()
        except BaseException:
            # also if closing failed: a writer that cannot close removes its file (or aborts its upload) itself,
            # and abort() is a no-op then
            logger.error('WARC writer failed, aborting the current WARC file')
            if writer is not None:
                await writer.abort()
            raise

        return {'stats': tracker.get_stats()}

    def log_reader(self, reader_id: int, counter: int, tracker: ThroughputTracker):
        """Log progress every N items."""
        if self.log_every_n > 0 and counter % self.log_every_n == 0:
            stats = tracker.get_stats()
            logger.info(
                'WARC Reader %d: %d items, %.1f MB, %.2f MB/s, %.2f req/s',
                reader_id,
                counter,
                stats['total_bytes'] / (1024 * 1024),
                stats['mb_per_sec'],
                stats['requests_per_sec'],
            )

    def log_writer(self, counter: int, tracker: ThroughputTracker):
        """Log progress every N items."""
        if self.log_every_n > 0 and counter % self.log_every_n == 0:
            stats = tracker.get_stats()
            logger.info(
                'WARC Writer: %d items, %.1f MB written, %.2f MB/s',
                counter,
                stats['total_bytes'] / (1024 * 1024),
                stats['mb_per_sec'],
            )

    async def start_file(self, sequence: int, **new_writer_kwargs):
        """Start a new WARC file with its warcinfo record (and the metadata records, if any).

        Returns the writer and the number of bytes written."""
        writer, file_size, warcinfo_id = await create_new_writer_with_header(sequence=sequence, **new_writer_kwargs)
        if self.write_paths_as_metadata_records:
            try:
                file_size += await self.write_metadata_records(writer, warcinfo_id=warcinfo_id)
            except BaseException:
                await writer.abort()
                raise
        return writer, file_size
