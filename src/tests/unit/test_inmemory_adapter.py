import pytest
import tempfile
import os
import time
from pathlib import Path
from src.pycache.adapters.InMemory import InMemory
from src.pycache.snapshot.Snapshot import SnapshotConfig
from src.pycache.datatypes.String import String


class TestInMemoryAdapter:
    """Unit tests for InMemory adapter with snapshot configuration"""

    def setup_method(self):
        """Set up test environment before each test"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test environment after each test"""
        # Clean up temporary files
        for file_path in Path(self.temp_dir).glob("*"):
            if file_path.is_file():
                file_path.unlink()

        try:
            os.rmdir(self.temp_dir)
        except:
            pass

    def test_inmemory_with_default_snapshot_config(self):
        """Test InMemory adapter with default snapshot configuration"""
        adapter = InMemory(connection_uri="")

        # Verify default snapshot config (uses default_config from Snapshot)
        from src.pycache.snapshot.Snapshot import default_config

        assert adapter.snapshot._config.dir == default_config.dir
        assert adapter.snapshot._config.min_changes == default_config.min_changes
        assert adapter.snapshot._config.auto == default_config.auto

        # Clean up
        try:
            adapter.snapshot.stop()
        except:
            pass

    def test_inmemory_with_custom_snapshot_config(self):
        """Test InMemory adapter with custom snapshot configuration"""
        custom_config = SnapshotConfig(
            dir=self.temp_dir,
            min_changes=5,
            interval_hours=2,
            auto=False,
            max_snapshots=10,
        )

        adapter = InMemory(connection_uri="", snapshot_config=custom_config)

        # Verify custom snapshot config is used
        assert adapter.snapshot._config == custom_config
        assert adapter.snapshot._config.dir == self.temp_dir
        assert adapter.snapshot._config.min_changes == 5
        assert adapter.snapshot._config.interval_hours == 2
        assert adapter.snapshot._config.auto == False
        assert adapter.snapshot._config.max_snapshots == 10

        # Verify snapshot directory is created
        assert Path(self.temp_dir).exists()
        assert Path(self.temp_dir).is_dir()

        # Clean up
        try:
            adapter.snapshot.stop()
        except:
            pass

    def test_inmemory_snapshot_functionality_with_custom_config(self):
        """Test that InMemory adapter works correctly with custom snapshot config"""
        custom_config = SnapshotConfig(
            dir=self.temp_dir,
            min_changes=1,  # Low threshold for testing
            interval_hours=1,
            auto=True,
            max_snapshots=5,
        )

        adapter = InMemory(connection_uri="", snapshot_config=custom_config)

        try:
            # Connect and start snapshot manager
            import asyncio

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Start adapter
            loop.run_until_complete(adapter.connect())

            # Set some values
            loop.run_until_complete(adapter.set("key1", String("value1")))
            loop.run_until_complete(adapter.set("key2", String("value2")))

            # Wait for snapshot to be created
            time.sleep(2)

            # Check that snapshot was created in custom directory
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1

            # Verify values can be retrieved
            value1 = loop.run_until_complete(adapter.get("key1", String("")))
            value2 = loop.run_until_complete(adapter.get("key2", String("")))

            assert value1.value == "value1"
            assert value2.value == "value2"

        finally:
            # Clean up
            try:
                loop.run_until_complete(adapter.close())
                loop.close()
            except:
                pass

    def test_inmemory_multiple_instances_different_configs(self):
        """Test that multiple InMemory instances can have different snapshot configs"""
        # Create first adapter with custom config
        config1 = SnapshotConfig(dir=self.temp_dir + "/snap1", min_changes=1)
        adapter1 = InMemory(connection_uri="", snapshot_config=config1)

        # Create second adapter with different custom config
        config2 = SnapshotConfig(dir=self.temp_dir + "/snap2", min_changes=2)
        adapter2 = InMemory(connection_uri="", snapshot_config=config2)

        # Verify they have different configs
        assert adapter1.snapshot._config != adapter2.snapshot._config
        assert adapter1.snapshot._config.dir != adapter2.snapshot._config.dir
        assert (
            adapter1.snapshot._config.min_changes
            != adapter2.snapshot._config.min_changes
        )

        # Verify directories are created
        assert Path(self.temp_dir + "/snap1").exists()
        assert Path(self.temp_dir + "/snap2").exists()

        # Clean up
        try:
            adapter1.snapshot.stop()
            adapter2.snapshot.stop()
        except:
            pass

    def test_inmemory_backward_compatibility(self):
        """Test that InMemory adapter maintains backward compatibility"""
        # Test without snapshot_config parameter (should use defaults)
        adapter = InMemory(connection_uri="")

        # Verify it still works as before
        assert adapter.snapshot is not None
        assert adapter.snapshot._config.dir == "./snapshot"

        # Clean up
        try:
            adapter.snapshot.stop()
        except:
            pass
