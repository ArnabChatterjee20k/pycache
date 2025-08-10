import pytest
from src.pycache import PyCache, Redis
from .base_cache_tests import BaseCacheTests


class TestRedisE2E(BaseCacheTests):
    def create_cache(self):
        # Use Redis database 1 for testing to avoid conflicts
        redis_adapter = Redis("redis://localhost:6379/1", tablename="test-cache")
        return PyCache(redis_adapter)

    def extract_value(self, datatype_instance):
        """Extract the native value from a datatype instance."""
        # For Redis adapter, values are already in native form
        if hasattr(datatype_instance, "value"):
            return datatype_instance.value
        return datatype_instance

    async def teardown_method(self):
        """Clean up after each test."""
        # Clean up the test database
        try:
            async with self.cache.session() as session:
                keys = await session.keys()
                for key in keys:
                    await session.delete(key)
        except:
            pass  # Ignore cleanup errors

    # Override tests that use get() without datatype to pass the expected datatype
    async def test_set_and_get_string(self):
        from src.pycache.datatypes import String
        async with self.cache.session() as cache:
            await cache.set("foo", String("bar"))
            result = await cache.get("foo", String(""))
            assert self.extract_value(result) == "bar" 