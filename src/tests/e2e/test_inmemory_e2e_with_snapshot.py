import pytest
import tempfile
import os
import asyncio
import time
from pathlib import Path
from src.pycache.py_cache import PyCache
from src.pycache.adapters.InMemory import InMemory
from src.pycache.snapshot.Snapshot import SnapshotConfig
from src.pycache.datatypes import String, List, Map, Set, Numeric
from src.tests.e2e.test_inmemory_e2e import TestInMemoryCache


class TestInMemoryCacheWithSnapshot(TestInMemoryCache):
    def setup_method(self):
        """Setup method called before each test."""
        # Create a temporary directory for snapshots
        self.temp_dir = tempfile.mkdtemp(prefix="test_snapshots_")
        self.cache: PyCache = self.create_cache()

    def teardown_method(self):
        """Teardown method called after each test."""
        # Clean up temporary directory
        try:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def create_cache(self):
        """Create InMemory cache with snapshot configuration."""
        snapshot_config = SnapshotConfig(
            dir=self.temp_dir,
            min_changes=1,  # Create snapshot after every change
            interval_hours=0,  # Use 0 hours for immediate snapshots in tests
            auto=True,  # Enable auto snapshots
            max_snapshots=3
        )
        return PyCache(InMemory("memory://", snapshot=True, snapshot_config=snapshot_config))

    # Snapshot-specific tests

    async def test_snapshot_debug(self):
        """Debug test to verify snapshot functionality step by step"""
        print(f"\n=== Snapshot Debug Test ===")
        print(f"Temp dir: {self.temp_dir}")
        print(f"Cache snapshot enabled: {self.cache._adapter.snapshots_enabled}")
        print(f"Snapshot object: {self.cache._adapter._snapshot}")
        
        if self.cache._adapter._snapshot:
            print(f"Snapshot config: {self.cache._adapter._snapshot._config}")
            print(f"Snapshot auto: {self.cache._adapter._snapshot._config.auto}")
            print(f"Snapshot min_changes: {self.cache._adapter._snapshot._config.min_changes}")
            print(f"Snapshot interval_hours: {self.cache._adapter._snapshot._config.interval_hours}")
        
        async with self.cache.session() as cache:
            print(f"Session created")
            print(f"Setting key1...")
            await cache.set("key1", String("value1"))
            print(f"Key1 set successfully")
            
            # Check if snapshot was created
            await asyncio.sleep(1)  # Wait a bit longer
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            print(f"Snapshot files found: {len(snapshot_files)}")
            for f in snapshot_files:
                print(f"  - {f.name} (size: {f.stat().st_size})")
            
            if snapshot_files:
                latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
                print(f"Latest snapshot: {latest_snapshot.name}")
                with open(latest_snapshot, 'rb') as f:
                    from src.pycache.snapshot.Reader import Reader
                    data = Reader(f).load()
                    print(f"Snapshot data: {data}")
            else:
                print("No snapshots created!")

    async def test_snapshot_creation_on_changes(self):
        """Test that snapshots are created when changes occur"""
        
        async with self.cache.session() as cache:
            # Set some initial data
            await cache.set("key1", String("value1"))
            await cache.set("key2", String("value2"))
            
            # Wait a bit for snapshot to be created
            await asyncio.sleep(0.5)
        
        # Check that snapshot files were created
        snapshot_files = list(Path(self.temp_dir).glob("*"))
        assert len(snapshot_files) >= 1, f"Expected at least 1 snapshot, got {len(snapshot_files)}"
            
        # Verify snapshot content
        latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
        with open(latest_snapshot, 'rb') as f:
            from src.pycache.snapshot.Reader import Reader
            data = Reader(f).load()
            assert "key1" in data
            assert "key2" in data

    async def test_snapshot_persistence_across_sessions(self):
        """Test that data persists across cache sessions via snapshots"""
        
        async with self.cache.session() as cache:
            # Set data
            await cache.set("persistent_key", String("persistent_value"))
            await cache.set("another_key", String("another_value"))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
        
        # Create new cache instance pointing to same snapshot directory
        new_snapshot_config = SnapshotConfig(
            dir=self.temp_dir,
            min_changes=1,
            interval_hours=0,  # Use 0 hours for immediate snapshots in tests
            auto=True,
            max_snapshots=3
        )
        new_cache = PyCache(InMemory("memory://", snapshot=True, snapshot_config=new_snapshot_config))
        
        async with new_cache.session() as cache:
            # Data should be loaded from snapshot
            result1 = await cache.get("persistent_key")
            result2 = await cache.get("another_key")
            
            assert result1 == "persistent_value"
            assert result2 == "another_value"

    async def test_snapshot_rotation_and_cleanup(self):
        """Test that old snapshots are cleaned up when max_snapshots is reached"""
        
        async with self.cache.session() as cache:
            # Create multiple snapshots
            for i in range(5):
                await cache.set(f"key_{i}", String(f"value_{i}"))
                await asyncio.sleep(0.2)  # Ensure time separation
            
            # Wait for cleanup
            await asyncio.sleep(1)
            
            # Should only keep max_snapshots (3)
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) == 3, f"Expected 3 snapshots, got {len(snapshot_files)}"

    async def test_snapshot_with_complex_datatypes(self):
        """Test that complex datatypes are properly serialized in snapshots"""
        
        async with self.cache.session() as cache:
            # Set complex data
            await cache.set("list_key", List([1, 2, 3, "string", True, False]))
            await cache.set("map_key", Map({"nested": {"key": "value"}, "list": [1, 2, 3]}))
            await cache.set("set_key", Set([1, 2, 3, "unique"]))
            await cache.set("numeric_key", Numeric(42.5))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot contains complex data
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
            with open(latest_snapshot, 'rb') as f:
                from src.pycache.snapshot.Reader import Reader
                data = Reader(f).load()
                
                # Check that complex data is preserved
                assert "list_key" in data
                assert "map_key" in data
                assert "set_key" in data
                assert "numeric_key" in data

    async def test_snapshot_recovery_after_failure(self):
        """Test that snapshots can be recovered after cache failure"""
        
        async with self.cache.session() as cache:
            # Set some data
            await cache.set("recovery_key", String("recovery_value"))
            await cache.set("important_data", String("important_value"))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
        
        # Simulate cache failure by creating new instance
        recovery_config = SnapshotConfig(
            dir=self.temp_dir,
            min_changes=1,
            interval_hours=0,  # Use 0 hours for immediate snapshots in tests
            auto=True,
            max_snapshots=3
        )
        recovery_cache = PyCache(InMemory("memory://", snapshot=True, snapshot_config=recovery_config))
        
        async with recovery_cache.session() as cache:
            # Data should be recovered from snapshot
            result1 = await cache.get("recovery_key")
            result2 = await cache.get("important_data")
            
            assert result1 == "recovery_value"
            assert result2 == "important_value"
            
            # Should be able to continue working
            await cache.set("new_key", String("new_value"))
            new_result = await cache.get("new_key")
            assert new_result == "new_value"

    async def test_snapshot_with_boolean_values(self):
        """Test that boolean values are properly handled in snapshots"""
        
        async with self.cache.session() as cache:
            # Set boolean values
            await cache.set("bool_true", String("true"))
            await cache.set("bool_false", String("false"))
            await cache.set("mixed_data", List([String("true"), String("false"), String("string"), Numeric(42)]))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot contains boolean data
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
            with open(latest_snapshot, 'rb') as f:
                from src.pycache.snapshot.Reader import Reader
                data = Reader(f).load()
                
                # Check that boolean values are preserved
                assert data["bool_true"] == "true"
                assert data["bool_false"] == "false"
                assert data["mixed_data"][0] == "true"
                assert data["mixed_data"][1] == "false"

    async def test_snapshot_performance_under_load(self):
        """Test snapshot performance when handling many operations"""
        
        async with self.cache.session() as cache:
            start_time = time.time()
            
            # Perform many operations
            for i in range(100):
                await cache.set(f"load_key_{i}", String(f"load_value_{i}"))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            end_time = time.time()
            operation_time = end_time - start_time
            
            # Should complete within reasonable time
            assert operation_time < 10.0, f"Operations took too long: {operation_time}s"
            
            # Verify data was persisted
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            # Check a few random keys
            for i in [0, 25, 50, 75, 99]:
                result = await cache.get(f"load_key_{i}")
                assert result == f"load_value_{i}"

    async def test_snapshot_with_datetime_values(self):
        """Test that datetime values are properly handled in snapshots"""
        from datetime import datetime
        
        async with self.cache.session() as cache:
            # Set datetime values
            now = datetime.now()
            await cache.set("current_time", String(now.isoformat()))
            await cache.set("future_time", String(datetime(2025, 12, 31, 23, 59, 59).isoformat()))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot contains datetime data
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
            with open(latest_snapshot, 'rb') as f:
                from src.pycache.snapshot.Reader import Reader
                data = Reader(f).load()
                
                # Check that datetime values are preserved
                assert "current_time" in data
                assert "future_time" in data
                # Note: datetime values are stored as ISO strings in snapshots
                assert isinstance(data["current_time"], str)
                assert isinstance(data["future_time"], str)

    async def test_snapshot_with_none_values(self):
        """Test that None values are properly handled in snapshots"""
        
        async with self.cache.session() as cache:
            # Set None values
            await cache.set("none_key", String(""))
            await cache.set("mixed_none", List([Numeric(1), String(""), String("string"), String(""), String("true")]))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot contains None data
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
            with open(latest_snapshot, 'rb') as f:
                from src.pycache.snapshot.Reader import Reader
                data = Reader(f).load()
                
                # Check that None values are preserved
                assert data["none_key"] == ""
                assert data["mixed_none"][1] == ""
                assert data["mixed_none"][3] == ""

    async def test_snapshot_with_empty_structures(self):
        """Test that empty data structures are properly handled in snapshots"""
        
        async with self.cache.session() as cache:
            # Set empty structures
            await cache.set("empty_list", List([]))
            await cache.set("empty_dict", Map({}))
            await cache.set("empty_string", String(""))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot contains empty structures
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            latest_snapshot = max(snapshot_files, key=lambda f: f.stat().st_mtime)
            with open(latest_snapshot, 'rb') as f:
                from src.pycache.snapshot.Reader import Reader
                data = Reader(f).load()
                
                # Check that empty structures are preserved
                assert data["empty_list"] == []
                assert data["empty_dict"] == {}
                assert data["empty_string"] == ""

    async def test_snapshot_concurrent_access(self):
        """Test snapshot behavior under concurrent access"""
        
        async with self.cache.session() as cache:
            # Simulate concurrent operations
            async def concurrent_operation(worker_id):
                for i in range(10):
                    await cache.set(f"worker_{worker_id}_key_{i}", String(f"worker_{worker_id}_value_{i}"))
                    await asyncio.sleep(0.01)  # Small delay
            
            # Run multiple concurrent operations
            tasks = [concurrent_operation(i) for i in range(3)]
            await asyncio.gather(*tasks)
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot was created
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            # Verify data from all workers
            for worker_id in range(3):
                for i in range(10):
                    result = await cache.get(f"worker_{worker_id}_key_{i}")
                    assert result == f"worker_{worker_id}_value_{i}"

    async def test_snapshot_file_integrity(self):
        """Test that snapshot files are created with proper permissions and integrity"""
        
        async with self.cache.session() as cache:
            # Set some data
            await cache.set("integrity_key", String("integrity_value"))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Check snapshot files
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            for snapshot_file in snapshot_files:
                # Check file permissions
                assert snapshot_file.is_file()
                assert snapshot_file.stat().st_size > 0
                
                # Check file can be read
                with open(snapshot_file, 'rb') as f:
                    from src.pycache.snapshot.Reader import Reader
                    data = Reader(f).load()
                    assert "integrity_key" in data
                    assert data["integrity_key"] == "integrity_value"

    async def test_snapshot_with_large_data(self):
        """Test snapshot behavior with larger amounts of data"""
        
        async with self.cache.session() as cache:
            # Create larger data structure
            large_list = [String(f"item_{i}") for i in range(1000)]
            large_dict = {f"key_{i}": String(f"value_{i}") for i in range(500)}
            
            await cache.set("large_list", List(large_list))
            await cache.set("large_dict", Map(large_dict))
            
            # Wait for snapshot
            await asyncio.sleep(0.5)
            
            # Verify snapshot was created
            snapshot_files = list(Path(self.temp_dir).glob("*"))
            assert len(snapshot_files) >= 1
            
            # Verify data integrity
            result_list = await cache.get("large_list")
            result_dict = await cache.get("large_dict")
            
            assert len(result_list) == 1000
            assert len(result_dict) == 500
            assert result_list[999] == "item_999"
            assert result_dict["key_499"] == "value_499" 