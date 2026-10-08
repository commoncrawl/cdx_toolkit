import asyncio
import contextlib
import logging
import os
import statistics
import sys
from typing import Callable, List, Optional, Dict


from cdx_toolkit.repackage.s3_utils import (
    is_s3_url,
)
from cdx_toolkit.repackage.hf_utils import is_hf_url, make_hf_reader
from cdx_toolkit.repackage.data_classes import RangeJob, RangePayload, ThroughputTracker
from cdx_toolkit.repackage.warc_utils import create_new_writer_with_header
from cdx_toolkit.repackage.sources.base import RangeJobSource
from cdx_toolkit.repackage.warc_utils import get_bytes_from_warc_record, get_metadata_record_from_path


_STOP = object()

logger = logging.getLogger(__name__)


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
        source: RangeJobSource,
        writer_subprefix: Optional[str] = None,
        write_paths_as_metadata_records: Optional[List[str]] = None,
        log_every_n: int = 1000,
        warc_download_prefix: Optional[str] = None,
        n_parallel_readers: int = 1,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
        # writer_kwargs: Optional[Dict] = None,
        range_jobs_queue_size: int = 1000,
        warc_records_queue_size: int = 200,
        aws_region_name: str = 'us-east-1',
        warc_version: str = '1.0',
        content_type: Optional[str] = None,
        min_part_size: int = 5 * 1024 * 1024,  # 5 MiB (for upload)
        max_file_size: Optional[int] = 1 * 1024 * 1024 * 1024,  # 1 GiB (for WARC outputs)
        next_file_sequence: Optional[Callable[[int], int]] = None,
    ):
        """Initialize the WARC filter.

        Args:
            source: RangeJobSource that yields the WARC ranges to repackage.
            prefix_path: Output path prefix for filtered WARC files.
            writer_info: Dictionary containing writer metadata.
            writer_subprefix: Optional subprefix for writer output paths.
            write_paths_as_metadata_records: Optional list of file paths to write as metadata records.
            log_every_n: Log progress every N records.
            warc_download_prefix: Optional prefix to prepend to WARC URLs.
            n_parallel_readers: Number of async readers per process (one writer per process).
            max_attempts: Maximum retry attempts for failed operations.
            base_backoff_seconds: Base backoff time in seconds for retries.
            writer_kwargs: Optional additional kwargs for writers.
            range_jobs_queue_size: Maximum size of range jobs queue.
            warc_records_queue_size: Maximum size of WARC records queue.
            aws_region_name: AWS region name for S3 operations.
            warc_version: WARC format version (e.g., '1.0' or '1.1').
            content_type: Optional content type for WARC output.
            min_part_size: Minimum part byte size for multipart uploads (default: 5 MiB).
            max_file_size: Maximum byte size for individual WARC files (default: 1 GiB).
            next_file_sequence: Number of the next output file, given the current one
                (default: current + 1).
        """
        self.source = source
        self.prefix_path = prefix_path
        self.writer_info = writer_info
        self.writer_subprefix = writer_subprefix
        self.write_paths_as_metadata_records = write_paths_as_metadata_records
        self.log_every_n = log_every_n
        self.warc_download_prefix = warc_download_prefix

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

    def filter(self) -> int:
        """Perform the filtering process (calls async method via asyncio.run).

        Returns:
            int: Number of records written, or -1 if interrupted.
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
            return runner(self.filter_async())
        except KeyboardInterrupt:
            logger.warning('Interrupted by user.')

        return -1

    def needs_aws(self) -> bool:
        """Returns true if the read/write (stage 2/3) S3 clients are needed.

        Sources own their own stage-1 resource (Athena client / DuckDB connection /
        fsspec), so this only concerns WARC reads and output writes.
        """
        return is_s3_url(self.warc_download_prefix) or is_s3_url(self.prefix_path)

    def get_boto3_base_config(self) -> Dict:
        """Get boto3 base configuration for AWS client.

        Returns:
            Dict: Boto3 base configuration object with retry and timeout settings.
        """
        # Calculate max connections based on parallelism
        # Each reader + writer needs connections, plus some overhead for retries
        # max_pool_connections = max(50, (self.num_readers + self.num_writers) * 2)

        return dict(
            region_name=self.aws_region_name,
            retries={
                'max_attempts': max(2, self.max_attempts),
                'mode': 'adaptive',  # Better than 'standard' for variable workloads
            },
        )

    async def get_aws_clients(self) -> Optional[Dict]:
        """Return async S3 clients for WARC reads/writes if needed.

        Stage-1 clients/connections are owned by the source, so this only builds the
        read/write S3 clients used to fetch WARC ranges and write output.

        Raises:
            SystemExit: If S3 is needed but Python version is < 3.9.
        """
        if self.needs_aws():
            if sys.version_info.major < 3 or (sys.version_info.major >= 3 and sys.version_info.minor < 9):
                logger.error('Reading and writing to S3 requires Python version >= 3.9')
                sys.exit(1)

            import aioboto3
            from botocore.config import Config

            session = aioboto3.Session()

            # High-throughput config for range reads
            read_config = Config(
                max_pool_connections=self.num_readers * 3,
                read_timeout=300,
                tcp_keepalive=True,
                **self.get_boto3_base_config(),
            )

            # Optimized config for multipart uploads (single writer)
            write_config = Config(
                max_pool_connections=8,
                read_timeout=120,
                connect_timeout=10,
                **self.get_boto3_base_config(),
            )

            return {
                'read': session.client('s3', config=read_config),
                'write': session.client('s3', config=write_config),
            }
        else:
            return None

    async def filter_async(self) -> int:
        """Filter process using a three stage approach (job generator, warc reader, warc writer).

        Returns:
            int: Number of records written.
        """
        range_jobs_queue: asyncio.Queue = asyncio.Queue(maxsize=self.range_jobs_queue_size)
        warc_records_queue: asyncio.Queue = asyncio.Queue(maxsize=self.warc_records_queue_size)

        read_is_hf = is_hf_url(self.warc_download_prefix)

        async with contextlib.AsyncExitStack() as stack:
            read_s3_client = write_s3_client = None
            # S3 read and/or write clients (built together when either side is S3).
            if self.needs_aws():
                clients = await self.get_aws_clients()
                read_s3_client = await stack.enter_async_context(clients['read'])
                write_s3_client = await stack.enter_async_context(clients['write'])

            # HF reader for hf:// downloads (CDN ranged GETs, one resolve per WARC file).
            hf_reader = None
            if read_is_hf:
                hf_reader = await stack.enter_async_context(make_hf_reader())

            return await self._run_filter_pipeline(
                range_jobs_queue=range_jobs_queue,
                warc_records_queue=warc_records_queue,
                read_s3_client=read_s3_client,
                write_s3_client=write_s3_client,
                hf_reader=hf_reader,
            )

    async def _produce_range_jobs(self, range_jobs_queue: asyncio.Queue) -> int:
        """Drive the (sync) source in a worker thread, feeding the async queue.

        Owns counting and emitting one _STOP sentinel per reader in a finally -- so
        readers never hang even if the source raises mid-iteration."""
        loop = asyncio.get_running_loop()
        count = 0

        def drain() -> int:
            nonlocal count
            for job in self.source.iter_range_jobs():
                asyncio.run_coroutine_threadsafe(range_jobs_queue.put(job), loop).result()
                count += 1
            return count

        try:
            await asyncio.to_thread(drain)
        finally:
            for _ in range(self.num_readers):
                await range_jobs_queue.put(_STOP)

        logger.info('Generated %d range jobs', count)
        return count

    async def _run_filter_pipeline(
        self,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        read_s3_client=None,
        write_s3_client=None,
        hf_reader=None,
    ) -> int:
        """Run the actual filter pipeline with or without S3 client.

        Args:
            range_jobs_queue: Queue for range jobs from the source.
            warc_records_queue: Queue for WARC record payloads.
            read_s3_client: Optional S3 client for reads from S3.
            write_s3_client: Optional S3 client for writes S3.
            hf_reader: Optional HF reader for hf:// downloads.

        Returns:
            int: Number of records written.
        """
        logger.info('Starting job generator, %d WARC readers, 1 WARC writer', self.num_readers)

        # Generate range jobs from the configured source (bridged sync->async in a thread).
        job_generators = asyncio.create_task(self._produce_range_jobs(range_jobs_queue))

        # Read WARC records based on file paths and ranges
        warc_readers = [
            asyncio.create_task(
                self.read_warc_records(
                    reader_id=i,
                    range_jobs_queue=range_jobs_queue,
                    warc_records_queue=warc_records_queue,
                    s3_client=read_s3_client,
                    hf_reader=hf_reader,
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

        # Start writer coordination task
        writer_coordinator = asyncio.create_task(self._coordinate_writer_shutdown(warc_readers, warc_records_queue))

        await job_generators
        logger.info('Range jobs submitted, monitoring readers and writer')

        # Wait for all tasks to complete
        readers_results = await asyncio.gather(*warc_readers)
        writer_result = await warc_writer
        await writer_coordinator

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

        return writer_stats['total_records']

    async def _coordinate_writer_shutdown(self, warc_readers: List[asyncio.Task], warc_records_queue: asyncio.Queue):
        """Coordinate efficient shutdown of writers as readers complete.

        This prevents writers from waiting unnecessarily when all readers are done
        and the records queue is being drained.
        """
        completed_readers = 0

        # Monitor reader completion
        while completed_readers < len(warc_readers):
            # Wait for any reader to complete
            done, pending = await asyncio.wait(
                warc_readers,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=1.0,  # Check periodically
            )

            if done:
                completed_readers = len(warc_readers) - len(pending)
                logger.debug(f'Readers completed: {completed_readers}/{len(warc_readers)}')

        # All readers completed - signal the writer to stop
        logger.info('All readers completed, signaling writer to stop')
        await warc_records_queue.put(_STOP)

    async def read_warc_records(
        self,
        reader_id: int,
        range_jobs_queue: asyncio.Queue,
        warc_records_queue: asyncio.Queue,
        s3_client=None,
        hf_reader=None,
    ) -> dict:
        """Read WARC records based on range jobs -> enqueue RangePayload.

        Args:
            reader_id: Unique identifier for this reader task.
            range_jobs_queue: Queue to read RangeJob objects from.
            warc_records_queue: Queue to put RangePayload objects into.
            s3_client: Optional S3 client for reading WARC files from S3.
            hf_reader: Optional HF reader for reading WARC ranges from hf:// buckets.

        Returns:
            dict: Statistics dictionary with reader_id and throughput stats.
        """
        tracker = ThroughputTracker()
        tracker.start()
        counter = 0

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
                    s3_client=s3_client,
                    hf_reader=hf_reader,
                )
                tracker.add(bytes_count=len(data), records_count=job.records_count)
                counter += 1

                # Log progress every N items
                self.log_reader(reader_id=reader_id, counter=counter, tracker=tracker)

                await warc_records_queue.put(RangePayload(job=job, data=data))
            except Exception:
                logger.exception(
                    'WARC Reader %d failed on %s/%s [%d,%d]',
                    reader_id,
                    getattr(job, 'bucket', '?'),
                    getattr(job, 'key', '?'),
                    getattr(job, 'offset', -1),
                    getattr(job, 'length', -1),
                )
            finally:
                range_jobs_queue.task_done()

        return {'reader_id': reader_id, 'stats': tracker.get_stats()}

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

        # Response records
        try:
            while True:
                item = await warc_records_queue.get()
                counter += 1
                try:
                    if item is _STOP:
                        stats = tracker.get_stats()
                        logger.info(
                            'WARC writer stopping. Stats: %.1fs, %d items, %.1f MB written, %.2f MB/s write speed',
                            stats['elapsed'],
                            stats['total_requests'],
                            stats['total_bytes'] / (1024 * 1024),
                            stats['mb_per_sec'],
                        )
                        should_stop = True
                    else:
                        should_stop = False
                        assert isinstance(item, RangePayload)

                        if writer is None:
                            # first file, with header and metadata records
                            current_file_sequence = self.next_file_sequence(0)
                            writer, current_file_size = await self.start_file(
                                current_file_sequence, **new_writer_kwargs
                            )

                        # Check if we need to rotate files due to size limit
                        writer, current_file_sequence, current_file_size = await self.rotate_files(
                            writer=writer,
                            current_file_sequence=current_file_sequence,
                            current_file_size=current_file_size,
                            added_byte_size=len(item.data),
                            **new_writer_kwargs,
                        )

                        # Write actual response record
                        await writer.write(item.data)
                        current_file_size += len(item.data)
                        tracker.add(bytes_count=len(item.data), records_count=item.job.records_count)

                        # Log progress every N items
                        self.log_writer(counter=counter, tracker=tracker)

                except Exception:
                    logger.exception('WARC writer failed on %s', getattr(item, 'job', None))
                    should_stop = False
                finally:
                    warc_records_queue.task_done()

                if should_stop:
                    break
        finally:
            if writer is not None:
                await writer.close()

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

    async def rotate_files(
        self, writer, current_file_sequence: int, current_file_size: int, added_byte_size: int, **new_writer_kwargs
    ):
        """Check if we need to rotate files due to size limit and perform rotation if needed."""
        if self.max_file_size and current_file_size + added_byte_size > self.max_file_size:
            await writer.close()
            current_file_sequence = self.next_file_sequence(current_file_sequence)
            writer, current_file_size = await self.start_file(current_file_sequence, **new_writer_kwargs)
            logger.info(f'Rotated to new WARC file sequence {current_file_sequence} due to size limit')

        return writer, current_file_sequence, current_file_size

    async def start_file(self, sequence: int, **new_writer_kwargs):
        """Start a new WARC file with its warcinfo record (and the metadata records, if any).

        Returns the writer and the number of bytes written."""
        writer, file_size, warcinfo_id = await create_new_writer_with_header(sequence=sequence, **new_writer_kwargs)
        if self.write_paths_as_metadata_records:
            file_size += await self.write_metadata_records(writer, warcinfo_id=warcinfo_id)
        return writer, file_size
