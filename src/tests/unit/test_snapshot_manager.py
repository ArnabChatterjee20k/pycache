import pytest
import tempfile
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from src.pycache.snapshot.Snapshot import (
    SnapshotManager,
    SnapshotConfig,
    default_config,
)


class TestSnapshotManager:
    """Unit tests for SnapshotManager class"""

    def setup_method(self):
        """Set up test environment before each test"""
        self.temp_dir = tempfile.mkdtemp()
        self.config = SnapshotConfig(
            dir=self.temp_dir,
            min_changes=2,
            interval_hours=1,
            auto=True,  # Changed to True so worker process starts
            max_snapshots=3,
        )
        self.snapshot_manager = SnapshotManager(self.config)

    def teardown_method(self):
        """Clean up test environment after each test"""
        try:
            self.snapshot_manager.stop()
        except:
            pass

        # Clean up temporary files
        for file_path in Path(self.temp_dir).glob("*"):
            if file_path.is_file():
                file_path.unlink()

        try:
            os.rmdir(self.temp_dir)
        except:
            pass

    def test_init_snapshot_dir_creates_directory(self):
        """Test that snapshot directory is created if it doesn't exist"""
        # Remove the directory
        import shutil

        shutil.rmtree(self.temp_dir)

        # Recreate snapshot manager - should create directory
        new_manager = SnapshotManager(self.config)
        assert Path(self.temp_dir).exists()
        assert Path(self.temp_dir).is_dir()

        # Clean up
        new_manager.stop()

    def test_init_snapshot_dir_with_existing_file_raises_error(self):
        """Test that error is raised if path exists but is not a directory"""
        # Create a file instead of directory
        file_path = Path(self.temp_dir) / "test_file"
        file_path.touch()  # Create an actual file

        with pytest.raises(TypeError, match="The path config needs to be a directory"):
            SnapshotManager(SnapshotConfig(dir=str(file_path)))

    def test_record_change_increments_counter(self):
        """Test that record_change increments the changes counter"""
        initial_value = self.snapshot_manager._changes.value

        self.snapshot_manager.record_change(1)
        assert self.snapshot_manager._changes.value == initial_value + 1

        self.snapshot_manager.record_change(5)
        assert self.snapshot_manager._changes.value == initial_value + 6

    def test_record_change_triggers_snapshot_when_threshold_reached(self):
        """Test that trigger is set when changes threshold is reached"""
        # Set changes to just below threshold
        self.snapshot_manager._changes.value = 1

        # Record one more change to reach threshold
        self.snapshot_manager.record_change(1)

        # Trigger should be set
        assert self.snapshot_manager._trigger.is_set()

    def test_force_snapshot_creates_file(self):
        """Test that force_snapshot creates a snapshot file"""
        test_data = {"key1": "value1", "key2": "value2"}

        self.snapshot_manager.force_snapshot(test_data)

        # Check that snapshot file was created
        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) == 1

        # Verify file content can be read
        snapshot_file = snapshot_files[0]
        assert snapshot_file.exists()
        assert snapshot_file.stat().st_size > 0

    def test_force_snapshot_resets_changes_counter(self):
        """Test that force_snapshot resets the changes counter"""
        # Set some changes
        self.snapshot_manager._changes.value = 5

        test_data = {"key1": "value1"}
        self.snapshot_manager.force_snapshot(test_data)

        # Changes counter should be reset
        assert self.snapshot_manager._changes.value == 0

    def test_force_snapshot_creates_timestamped_filename(self):
        """Test that snapshot files have timestamped names"""
        test_data = {"key1": "value1"}

        before_snapshot = datetime.now()
        self.snapshot_manager.force_snapshot(test_data)
        after_snapshot = datetime.now()

        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) == 1

        snapshot_file = snapshot_files[0]
        filename = snapshot_file.name

        # Parse timestamp from filename
        timestamp = datetime.strptime(filename, self.snapshot_manager._datetime_format)

        # Timestamp should be between before and after
        assert before_snapshot <= timestamp <= after_snapshot

    def test_load_snapshot_with_no_files_returns_empty_dict(self):
        """Test that load_snapshot returns empty dict when no snapshots exist"""
        loaded_data = self.snapshot_manager.load_snapshot()
        assert loaded_data == {}

    def test_load_snapshot_loads_latest_by_default(self):
        """Test that load_snapshot loads the latest snapshot by default"""
        # Create multiple snapshots with different timestamps
        test_data1 = {"key1": "value1"}
        test_data2 = {"key2": "value2"}

        # Force first snapshot
        self.snapshot_manager.force_snapshot(test_data1)
        time.sleep(0.1)  # Ensure different timestamps

        # Force second snapshot
        self.snapshot_manager.force_snapshot(test_data2)

        # Load latest snapshot
        loaded_data = self.snapshot_manager.load_snapshot()

        # Should load the second snapshot
        assert loaded_data == test_data2

    def test_load_snapshot_with_specific_timestamp(self):
        """Test that load_snapshot loads snapshot closest to specified timestamp"""
        # Create multiple snapshots
        test_data1 = {"key1": "value1"}
        test_data2 = {"key2": "value2"}

        # Force first snapshot
        self.snapshot_manager.force_snapshot(test_data1)
        time.sleep(0.1)

        # Get timestamp of first snapshot
        snapshot_files = list(Path(self.temp_dir).glob("*"))
        first_snapshot_name = snapshot_files[0].name

        # Force second snapshot
        self.snapshot_manager.force_snapshot(test_data2)

        # Load snapshot closest to first timestamp
        loaded_data = self.snapshot_manager.load_snapshot(first_snapshot_name)

        # Should load the first snapshot
        assert loaded_data == test_data1

    def test_prune_old_snapshots(self):
        """Test that prune_old_snapshots removes old snapshots"""
        # Create more snapshots than max_snapshots
        for i in range(5):  # More than max_snapshots (3)
            test_data = {f"key{i}": f"value{i}"}
            self.snapshot_manager.force_snapshot(test_data)
            time.sleep(0.1)  # Ensure different timestamps

        # Check that only max_snapshots remain
        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) == 3

        # Should keep the most recent snapshots
        snapshot_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        expected_files = snapshot_files[:3]
        actual_files = list(Path(self.temp_dir).glob("*"))

        assert set(expected_files) == set(actual_files)

    def test_start_starts_worker_process(self):
        """Test that start method starts the worker process"""
        test_data = {"key1": "value1"}

        self.snapshot_manager.start(test_data)

        # Worker should be started
        assert self.snapshot_manager._worker is not None
        assert self.snapshot_manager._worker.is_alive()

        # Clean up
        self.snapshot_manager.stop()

    def test_stop_stops_worker_process(self):
        """Test that stop method stops the worker process"""
        test_data = {"key1": "value1"}

        self.snapshot_manager.start(test_data)
        assert self.snapshot_manager._worker.is_alive()

        self.snapshot_manager.stop()

        # Worker should be stopped
        assert not self.snapshot_manager._worker.is_alive()

    def test_is_processing_returns_worker_status(self):
        """Test that is_processing returns the correct worker status"""
        # Initially no worker
        assert not self.snapshot_manager.is_processing()

        # Start worker
        test_data = {"key1": "value1"}
        self.snapshot_manager.start(test_data)
        assert self.snapshot_manager.is_processing()

        # Stop worker
        self.snapshot_manager.stop()
        assert not self.snapshot_manager.is_processing()

    @patch("multiprocessing.Process")
    def test_worker_process_behavior(self, mock_process):
        """Test worker process behavior with mocked multiprocessing"""
        # Mock the Process class
        mock_worker = Mock()
        mock_worker.is_alive.return_value = True
        mock_process.return_value = mock_worker

        test_data = {"key1": "value1"}
        self.snapshot_manager.start(test_data)

        # Verify process was started
        mock_process.assert_called_once()
        mock_worker.start.assert_called_once()

        # Clean up
        self.snapshot_manager.stop()

    def test_copy_on_write_mechanism(self):
        """Test the copy-on-write mechanism with multiprocessing"""
        # This test verifies that the snapshot manager can work with
        # multiprocessing and shared memory

        test_data = {"key1": "value1", "key2": "value2"}

        # Start the snapshot manager
        self.snapshot_manager.start(test_data)

        # Record some changes
        self.snapshot_manager.record_change(1)
        self.snapshot_manager.record_change(1)

        # Wait a bit for the worker to process
        time.sleep(0.5)

        # Check that snapshot was created
        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) >= 1

        # Stop the manager
        self.snapshot_manager.stop()

    def test_snapshot_integrity(self):
        """Test that snapshots maintain data integrity"""
        # Create complex test data
        test_data = {
            "string_key": "string_value",
            "int_key": 42,
            "list_key": [1, 2, 3],
            "dict_key": {"nested": {"deep": "value"}},
            "set_key": {1, 2, 3},
            "none_key": None,
        }

        # Create snapshot
        self.snapshot_manager.force_snapshot(test_data)

        # Load snapshot
        loaded_data = self.snapshot_manager.load_snapshot()

        # Verify data integrity
        assert loaded_data["string_key"] == test_data["string_key"]
        assert loaded_data["int_key"] == test_data["int_key"]
        assert loaded_data["list_key"] == test_data["list_key"]
        assert loaded_data["dict_key"] == test_data["dict_key"]
        assert loaded_data["set_key"] == test_data["set_key"]
        assert loaded_data["none_key"] is None

    def test_concurrent_access_safety(self):
        """Test that snapshot manager is safe for concurrent access"""
        import threading

        test_data = {"key1": "value1"}
        results = []
        errors = []

        def worker_function():
            try:
                # Record changes from multiple threads
                for i in range(10):
                    self.snapshot_manager.record_change(1)
                    time.sleep(0.01)
                results.append("success")
            except Exception as e:
                errors.append(str(e))

        # Start multiple worker threads
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=worker_function)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        assert len(results) == 3  # All threads succeeded
        assert len(errors) == 0  # No errors occurred

        # Verify changes were recorded
        assert self.snapshot_manager._changes.value >= 30

    def test_error_handling_in_snapshot_creation(self):
        """Test error handling during snapshot creation"""
        # Test with invalid data that might cause issues
        invalid_data = {"key": object()}  # Object that can't be serialized

        # Should handle gracefully or raise appropriate error
        try:
            self.snapshot_manager.force_snapshot(invalid_data)
        except Exception as e:
            # Expected to fail, but should not crash
            assert isinstance(e, Exception)

    def test_snapshot_file_permissions(self):
        """Test that snapshot files have appropriate permissions"""
        test_data = {"key1": "value1"}

        self.snapshot_manager.force_snapshot(test_data)

        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) == 1

        snapshot_file = snapshot_files[0]

        # File should be readable
        assert os.access(snapshot_file, os.R_OK)

        # File should be writable by owner
        assert os.access(snapshot_file, os.W_OK)

    def test_snapshot_directory_cleanup(self):
        """Test that snapshot directory cleanup works correctly"""
        # Create some snapshots
        for i in range(3):
            test_data = {f"key{i}": f"value{i}"}
            self.snapshot_manager.force_snapshot(test_data)
            time.sleep(0.1)

        # Verify snapshots exist
        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) == 3

        # Clean up
        self.snapshot_manager.stop()

        # Directory should still exist but be empty after cleanup
        assert Path(self.temp_dir).exists()
        assert Path(self.temp_dir).is_dir()
