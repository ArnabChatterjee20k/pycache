from src.pycache.py_cache import PyCache
from src.pycache.adapters.SQLite import SQLite
from src.tests.e2e.base_cache_tests import FileBasedCacheTests


class TestSQLiteCache(FileBasedCacheTests):
    def create_cache(self):
        """Create SQLite cache with temporary database file."""
        db_path = self.create_temp_file()
        return PyCache(SQLite(db_path))
