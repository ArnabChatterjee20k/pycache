from src.pycache.py_cache import PyCache
from src.pycache.adapters.InMemory import InMemory
from src.tests.e2e.base_cache_tests import BaseCacheTests


class TestInMemoryCache(BaseCacheTests):
    def create_cache(self):
        """Create InMemory cache with memory URI."""
        return PyCache(InMemory("memory://"))
