import asyncio

from unittest.mock import AsyncMock, patch
from cdx_toolkit.repackage.data_classes import ThroughputTracker
from tests.conftest import TEST_DATA_PATH

from cdx_toolkit.repackage.warc_filter import WARCFilter
from cdx_toolkit.repackage.sources.records import RecordsSource

fixture_path = TEST_DATA_PATH / 'repackage'

# A throwaway source for unit tests that only exercise reader/writer/rotate/log methods.
_FAKE_SOURCE = RecordsSource([], 'https://data.commoncrawl.org')


def test_filter_keyboard_interrupt_handling(caplog):
    """Test that KeyboardInterrupt is properly handled in the filter method."""
    import logging

    # Set log level to capture WARNING messages
    caplog.set_level(logging.WARNING, logger='cdx_toolkit.repackage.warc_filter')

    warc_filter = WARCFilter(source=_FAKE_SOURCE, prefix_path='/fake/prefix', writer_info={'writer_id': 1})

    # Mock filter_async to raise KeyboardInterrupt
    with patch.object(warc_filter, 'filter_async', side_effect=KeyboardInterrupt('Simulated user interrupt')):
        # Call the filter method
        result = warc_filter.filter()

        # Should return -1 when interrupted
        assert result == -1

        # Should log the warning message
        assert 'Interrupted by user.' in caplog.text


def test_rotate_files_no_rotation_needed():
    """Test rotate_files when no rotation is needed (file size below limit)."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE,
            prefix_path='/fake/prefix',
            writer_info={'writer_id': 1},
            max_file_size=1000,  # 1KB limit
        )

        mock_writer = AsyncMock()
        current_file_sequence = 1
        current_file_size = 500  # 500 bytes
        added_byte_size = 200  # Adding 200 bytes, total would be 700 (below limit)

        # Call rotate_files
        result_writer, result_sequence, result_size = await warc_filter.rotate_files(
            writer=mock_writer,
            current_file_sequence=current_file_sequence,
            current_file_size=current_file_size,
            added_byte_size=added_byte_size,
            output_path_prefix='/fake/output',
            max_attempts=3,
            base_backoff_seconds=1.0,
            min_part_size=1024,
            writer_info={'writer_id': 1},
        )

        # Should return original values since no rotation occurred
        assert result_writer == mock_writer
        assert result_sequence == current_file_sequence
        assert result_size == current_file_size

        # Writer should not be closed
        mock_writer.close.assert_not_called()

    asyncio.run(run_test())


def test_rotate_files_rotation_needed_without_resource_records():
    """Test rotate_files when rotation is needed and no resource records to write."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE,
            prefix_path='/fake/prefix',
            writer_info={'writer_id': 1},
            max_file_size=1000,  # 1KB limit
            write_paths_as_metadata_records=None,  # No resource records
        )

        mock_writer = AsyncMock()
        mock_new_writer = AsyncMock()
        current_file_sequence = 1
        current_file_size = 800  # 800 bytes
        added_byte_size = 300  # Adding 300 bytes, total would be 1100 (above limit)

        # Mock create_new_writer_with_header
        with patch('cdx_toolkit.repackage.warc_filter.create_new_writer_with_header') as mock_create:
            mock_create.return_value = (mock_new_writer, 150, 'warcinfo-123')  # (writer, header_size, warcinfo_id)

            # Call rotate_files
            result_writer, result_sequence, result_size = await warc_filter.rotate_files(
                writer=mock_writer,
                current_file_sequence=current_file_sequence,
                current_file_size=current_file_size,
                added_byte_size=added_byte_size,
                output_path_prefix='/fake/output',
                max_attempts=3,
                base_backoff_seconds=1.0,
                min_part_size=1024,
                writer_info={'writer_id': 1},
            )

            # Should have rotated
            assert result_writer == mock_new_writer
            assert result_sequence == current_file_sequence + 1  # Incremented
            assert result_size == 150  # Header size only

            # Old writer should be closed
            mock_writer.close.assert_called_once()

            # New writer should be created
            mock_create.assert_called_once_with(
                sequence=current_file_sequence + 1,
                output_path_prefix='/fake/output',
                max_attempts=3,
                base_backoff_seconds=1.0,
                min_part_size=1024,
                writer_info={'writer_id': 1},
            )

    asyncio.run(run_test())


def test_rotate_files_rotation_needed_with_metadata_records():
    """Test rotate_files when rotation is needed and metadata records need to be written."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE,
            prefix_path='/fake/prefix',
            writer_info={'writer_id': 1},
            max_file_size=1000,  # 1KB limit
            write_paths_as_metadata_records=['/fake/resource1.txt', '/fake/resource2.txt'],
        )

        mock_writer = AsyncMock()
        mock_new_writer = AsyncMock()
        current_file_sequence = 1
        current_file_size = 800  # 800 bytes
        added_byte_size = 300  # Adding 300 bytes, total would be 1100 (above limit)

        # Mock create_new_writer_with_header
        with patch('cdx_toolkit.repackage.warc_filter.create_new_writer_with_header') as mock_create:
            mock_create.return_value = (mock_new_writer, 150, 'warcinfo-123')

            # Mock write_metadata_records
            with patch.object(warc_filter, 'write_metadata_records', return_value=75) as mock_write_resources:
                # Call rotate_files
                result_writer, result_sequence, result_size = await warc_filter.rotate_files(
                    writer=mock_writer,
                    current_file_sequence=current_file_sequence,
                    current_file_size=current_file_size,
                    added_byte_size=added_byte_size,
                    output_path_prefix='/fake/output',
                    max_attempts=3,
                    base_backoff_seconds=1.0,
                    min_part_size=1024,
                    writer_info={'writer_id': 1},
                )

                # Should have rotated
                assert result_writer == mock_new_writer
                assert result_sequence == current_file_sequence + 1
                assert result_size == 150 + 75  # Header size + resource records size

                # Old writer should be closed
                mock_writer.close.assert_called_once()

                # New writer should be created
                mock_create.assert_called_once()

                # Resource records should be written
                mock_write_resources.assert_called_once_with(mock_new_writer, warcinfo_id='warcinfo-123')

    asyncio.run(run_test())


def test_rotate_files_no_max_file_size_set():
    """Test rotate_files when max_file_size is not set (None)."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE,
            prefix_path='/fake/prefix',
            writer_info={'writer_id': 1},
            max_file_size=None,  # No limit
        )

        mock_writer = AsyncMock()
        current_file_sequence = 1
        current_file_size = 999999999  # Very large file
        added_byte_size = 999999999  # Very large addition

        # Call rotate_files
        result_writer, result_sequence, result_size = await warc_filter.rotate_files(
            writer=mock_writer,
            current_file_sequence=current_file_sequence,
            current_file_size=current_file_size,
            added_byte_size=added_byte_size,
            output_path_prefix='/fake/output',
            max_attempts=3,
            base_backoff_seconds=1.0,
            min_part_size=1024,
            writer_info={'writer_id': 1},
        )

        # Should not rotate regardless of size
        assert result_writer == mock_writer
        assert result_sequence == current_file_sequence
        assert result_size == current_file_size

        # Writer should not be closed
        mock_writer.close.assert_not_called()

    asyncio.run(run_test())


def test_rotate_files_edge_case_exact_limit():
    """Test rotate_files when the total size exactly equals the limit."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE,
            prefix_path='/fake/prefix',
            writer_info={'writer_id': 1},
            max_file_size=1000,  # 1KB limit
        )

        mock_writer = AsyncMock()
        current_file_sequence = 1
        current_file_size = 700  # 700 bytes
        added_byte_size = 300  # Adding 300 bytes, total would be exactly 1000

        # Call rotate_files
        result_writer, result_sequence, result_size = await warc_filter.rotate_files(
            writer=mock_writer,
            current_file_sequence=current_file_sequence,
            current_file_size=current_file_size,
            added_byte_size=added_byte_size,
            output_path_prefix='/fake/output',
            max_attempts=3,
            base_backoff_seconds=1.0,
            min_part_size=1024,
            writer_info={'writer_id': 1},
        )

        # Should not rotate when exactly at limit (only rotate when > limit)
        assert result_writer == mock_writer
        assert result_sequence == current_file_sequence
        assert result_size == current_file_size

        # Writer should not be closed
        mock_writer.close.assert_not_called()

    asyncio.run(run_test())


def test_rotate_files_edge_case_just_over_limit():
    """Test rotate_files when the total size is just 1 byte over the limit."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE,
            prefix_path='/fake/prefix',
            writer_info={'writer_id': 1},
            max_file_size=1000,  # 1KB limit
        )

        mock_writer = AsyncMock()
        mock_new_writer = AsyncMock()
        current_file_sequence = 1
        current_file_size = 700  # 700 bytes
        added_byte_size = 301  # Adding 301 bytes, total would be 1001 (1 byte over)

        # Mock create_new_writer_with_header
        with patch('cdx_toolkit.repackage.warc_filter.create_new_writer_with_header') as mock_create:
            mock_create.return_value = (mock_new_writer, 150, 'warcinfo-123')

            # Call rotate_files
            result_writer, result_sequence, result_size = await warc_filter.rotate_files(
                writer=mock_writer,
                current_file_sequence=current_file_sequence,
                current_file_size=current_file_size,
                added_byte_size=added_byte_size,
                output_path_prefix='/fake/output',
                max_attempts=3,
                base_backoff_seconds=1.0,
                min_part_size=1024,
                writer_info={'writer_id': 1},
            )

            # Should rotate when just over limit
            assert result_writer == mock_new_writer
            assert result_sequence == current_file_sequence + 1
            assert result_size == 150

            # Old writer should be closed
            mock_writer.close.assert_called_once()

    asyncio.run(run_test())


def test_rotate_files_kwargs_passed_through():
    """Test that all kwargs are properly passed to create_new_writer_with_header."""

    async def run_test():
        warc_filter = WARCFilter(
            source=_FAKE_SOURCE, prefix_path='/fake/prefix', writer_info={'writer_id': 1}, max_file_size=1000
        )

        mock_writer = AsyncMock()
        mock_new_writer = AsyncMock()

        # Mock create_new_writer_with_header
        with patch('cdx_toolkit.repackage.warc_filter.create_new_writer_with_header') as mock_create:
            mock_create.return_value = (mock_new_writer, 150, 'warcinfo-123')

            # Call rotate_files with various kwargs
            await warc_filter.rotate_files(
                writer=mock_writer,
                current_file_sequence=1,
                current_file_size=800,
                added_byte_size=300,
                output_path_prefix='/custom/output',
                max_attempts=5,
                base_backoff_seconds=2.5,
                min_part_size=2048,
                writer_info={'custom': 'info'},
                warc_version='1.1',
                gzip=True,
                custom_param='custom_value',
            )

            # Verify all kwargs are passed through
            mock_create.assert_called_once_with(
                sequence=2,  # incremented from 1
                output_path_prefix='/custom/output',
                max_attempts=5,
                base_backoff_seconds=2.5,
                min_part_size=2048,
                writer_info={'custom': 'info'},
                warc_version='1.1',
                gzip=True,
                custom_param='custom_value',
            )

    asyncio.run(run_test())


def test_rotate_files_logging(caplog):
    """Test that rotation logs the appropriate message."""
    import logging

    async def run_test():
        # Set log level to capture INFO messages
        caplog.set_level(logging.INFO, logger='cdx_toolkit.repackage.warc_filter')

        warc_filter = WARCFilter(
            source=_FAKE_SOURCE, prefix_path='/fake/prefix', writer_info={'writer_id': 1}, max_file_size=1000
        )

        mock_writer = AsyncMock()
        mock_new_writer = AsyncMock()

        # Mock create_new_writer_with_header
        with patch('cdx_toolkit.repackage.warc_filter.create_new_writer_with_header') as mock_create:
            mock_create.return_value = (mock_new_writer, 150, 'warcinfo-123')

            # Call rotate_files to trigger rotation
            await warc_filter.rotate_files(
                writer=mock_writer,
                current_file_sequence=5,
                current_file_size=800,
                added_byte_size=300,
                output_path_prefix='/fake/output',
                max_attempts=3,
                base_backoff_seconds=1.0,
                min_part_size=1024,
                writer_info={'writer_id': 1},
            )

            # Check that the rotation log message was written
            assert 'Rotated to new WARC file sequence 6 due to size limit' in caplog.text

    asyncio.run(run_test())


def test_log_writer(caplog):
    """Test log writer."""
    import logging
    caplog.set_level(logging.INFO, logger='cdx_toolkit.repackage.warc_filter')

    warc_filter = WARCFilter(
        source=_FAKE_SOURCE,
        prefix_path='/fake/prefix',
        writer_info={'writer_id': 1},
        log_every_n=2,
    )
    tracker = ThroughputTracker()
    warc_filter.log_writer(0, tracker)
    warc_filter.log_writer(1, tracker)
    warc_filter.log_writer(2, tracker)

    assert caplog.text.count('WARC Writer') == 2


def test_log_reader(caplog):
    """Test log reader."""
    import logging
    caplog.set_level(logging.INFO, logger='cdx_toolkit.repackage.warc_filter')

    warc_filter = WARCFilter(
        source=_FAKE_SOURCE,
        prefix_path='/fake/prefix',
        writer_info={'writer_id': 1},
        log_every_n=2,
    )
    tracker = ThroughputTracker()
    warc_filter.log_reader(1, 0, tracker)
    warc_filter.log_reader(1, 1, tracker)
    warc_filter.log_reader(1, 2, tracker)

    assert caplog.text.count('WARC Reader 1') == 2
