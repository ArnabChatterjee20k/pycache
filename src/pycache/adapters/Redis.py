import redis.asyncio as redis
import json
import logging
from collections import deque
from .Adapter import Adapter
from ..datatypes import String, List, Map, Numeric, Set, Queue, Streams
from ..datatypes.Datatype import Datatype

logger = logging.getLogger(__name__)


class Redis(Adapter):
    """
    EXPERIMENTAL: Redis adapter with native datatype support.
    This adapter maps Python datatypes to Redis data structures without serialization.
    """

    def __init__(self, connection_uri, pool_size=5, tablename="kv-store"):
        self._pool = redis.ConnectionPool.from_url(
            connection_uri, max_connections=pool_size, decode_responses=True
        )
        self._client: redis.Redis = redis.Redis(
            connection_pool=self._pool, decode_responses=True
        )
        self.client: redis.Redis = redis.Redis(
            connection_pool=self._pool, decode_responses=True
        )
        self.tablename = tablename
        self._transaction_pipeline = None
        self._original_client = None

    async def connect(self):
        return self

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, *args):
        return await self.close()

    async def close(self):
        return await self._client.aclose()

    async def create(self):
        return

    async def create_index(self):
        return

    def get_key_path(self, key: str) -> str:
        return f"{self.tablename}:{key}"

    def _get_client(self):
        return (
            self._transaction_pipeline
            if self._transaction_pipeline is not None
            else self._client
        )

    async def get(self, key: str, datatype: Datatype = None):
        key_path = self.get_key_path(key)
        result = await self._get_datatype(key_path, datatype.get_name())
        return result

    async def set(self, key, value):
        key_path = self.get_key_path(key)
        return await self._set_datatype(key_path, value)

    async def batch_get(self, keys, datatype: Datatype = None):
        if isinstance(keys, dict):
            # Keys is a dictionary {key: datatype}
            pipe = self._client.pipeline()
            key_order = []

            for original_key, datatype in keys.items():
                key_path = self.get_key_path(original_key)
                datatype_name = (
                    datatype.get_name()
                    if hasattr(datatype, "get_name")
                    else str(datatype)
                )
                key_order.append((original_key, key_path, datatype_name))
                await self._get_datatype(
                    key_path, datatype_name, pipe
                )  # Still need await, but returns None immediately

            raw_values = await pipe.execute()

            # Process results and map back to original keys
            final_result = {}
            for i, (original_key, key_path, datatype_name) in enumerate(key_order):
                final_result[original_key] = self._process_datatype_value(
                    raw_values[i], datatype_name
                )

            return final_result

        elif isinstance(keys, list):
            # Keys is a list of strings
            if datatype is None:
                datatype = String  # Default to String class if not specified

            # Get the datatype name from the Datatype instance
            datatype_name = datatype.get_name()

            if len(keys) == 1:
                # Single key - no need for pipeline
                key_path = self.get_key_path(keys[0])
                value = await self._get_datatype(key_path, datatype_name)
                return {keys[0]: value}
            else:
                # Multiple keys - use pipeline
                pipe = self._client.pipeline()
                key_paths = []

                for original_key in keys:
                    key_path = self.get_key_path(original_key)
                    key_paths.append((original_key, key_path))
                    await self._get_datatype(
                        key_path, datatype_name, pipe
                    )  # Still need await, but returns None immediately

                raw_values = await pipe.execute()

                # Process results and map back to original keys
                final_result = {}
                for i, (original_key, key_path) in enumerate(key_paths):
                    final_result[original_key] = self._process_datatype_value(
                        raw_values[i], datatype_name
                    )

                return final_result
        else:
            raise ValueError(
                "keys must be either a list of strings or a dict mapping keys to datatype names"
            )

    async def _get_datatype(self, key_path: str, datatype_name: str, pipe=None):
        client = pipe if pipe is not None else self._get_client()

        using_pipeline = pipe is not None or self._transaction_pipeline is not None

        if datatype_name == String.get_name():
            result = client.get(key_path)
        elif datatype_name == Numeric.get_name():
            result = client.get(key_path)
        elif datatype_name == List.get_name():
            result = client.lrange(key_path, 0, -1)
        elif datatype_name == Queue.get_name():
            result = client.lrange(key_path, 0, -1)
        elif datatype_name == Set.get_name():
            result = client.smembers(key_path)
        elif datatype_name == Map.get_name():
            result = client.hgetall(key_path)
        elif datatype_name == Streams.get_name():
            result = client.xrange(key_path)

        # if using pipeline, then result not awaitable
        if using_pipeline:
            return None

        raw_value = await result
        return self._process_datatype_value(raw_value, datatype_name)

    def _process_datatype_value(self, raw_value: str, datatype_name: str):
        if raw_value is None:
            return None

        if datatype_name == String.get_name():
            return raw_value

        elif datatype_name == Numeric.get_name():
            if "." in raw_value:
                return float(raw_value)
            return int(raw_value)

        elif datatype_name == List.get_name():
            return raw_value if raw_value else []

        elif datatype_name == Queue.get_name():
            return deque(raw_value) if raw_value else deque()

        elif datatype_name == Set.get_name():
            return set(raw_value) if raw_value else set()

        elif datatype_name == Map.get_name():
            return dict(raw_value) if raw_value else {}

        elif datatype_name == Streams.get_name():
            return Streams(raw_value).value

        else:
            return raw_value

    async def _set_datatype(self, key_path, datatype, pipe=None):
        value = datatype.value
        datatype_name = datatype.get_name()
        client = pipe if pipe is not None else self._get_client()

        using_pipeline = pipe is not None or self._transaction_pipeline is not None
        result = None

        try:
            # for sequence types, value must be present
            if datatype_name not in (String.get_name(), Numeric.get_name()) and not len(
                value
            ):
                return 0

            if datatype_name == String.get_name():
                result = client.set(key_path, str(value))

            elif datatype_name == Numeric.get_name():
                result = client.set(key_path, str(value))

            elif datatype_name in (List.get_name(), Queue.get_name()):
                str_values = [str(v) for v in value]
                result = client.lpush(key_path, *reversed(str_values))

            elif datatype_name == Set.get_name():
                str_values = [str(v) for v in value]
                result = client.sadd(key_path, *str_values)

            elif datatype_name == Map.get_name():
                str_mapping = {str(k): str(v) for k, v in value.items()}
                result = client.hset(key_path, mapping=str_mapping)

            elif datatype_name == Streams.get_name():
                stream_data = {}

                # Combine all entries into one field map
                for entry in value:
                    if isinstance(entry, (list, tuple)) and len(entry) == 2:
                        stream_data[str(entry[0])] = str(entry[1])
                    elif isinstance(entry, dict):
                        stream_data.update({str(k): str(v) for k, v in entry.items()})
                    else:
                        raise ValueError(f"Invalid stream entry format: {entry}")

                if stream_data:
                    result = client.xadd(key_path, stream_data)
            else:
                result = client.set(key_path, str(value))

            # if pipeline then no await
            if using_pipeline:
                return 1

            # if not streams
            if result is not None:
                return await result
            else:
                return 1

        except Exception as e:
            logger.error(
                f"Error setting datatype {datatype_name} for key {key_path}: {e}"
            )
            raise

    async def batch_set(self, key_values: dict[str, Datatype]) -> int:
        failed_keys = []

        if self._transaction_pipeline is not None:
            count = 0
            for key, datatype in key_values.items():
                try:
                    await self._set_datatype(
                        self.get_key_path(key), datatype, self._transaction_pipeline
                    )
                    count += 1
                except Exception as e:
                    logger.warning(f"Failed to set key {key} in transaction: {e}")
                    failed_keys.append(key)
            return count
        else:
            async with self._client.pipeline() as pipe:
                count = 0
                for key, datatype in key_values.items():
                    try:
                        await self._set_datatype(self.get_key_path(key), datatype, pipe)
                        count += 1
                    except Exception as e:
                        logger.warning(
                            f"Failed to queue set operation for key {key}: {e}"
                        )
                        failed_keys.append(key)

                try:
                    await pipe.execute()
                except Exception as e:
                    logger.error(f"Failed to execute batch set pipeline: {e}")
                    raise

                if failed_keys:
                    logger.warning(
                        f"Failed to set {len(failed_keys)} keys: {failed_keys}"
                    )

                return count

    async def delete(self, key: str) -> int:
        key_path = self.get_key_path(key)
        client = self._get_client()
        return await client.delete(key_path)

    async def exists(self, key: str) -> bool:
        key_path = self.get_key_path(key)
        client = self._get_client()

        return await client.exists(key_path)

    async def keys(self) -> list:
        client = self._get_client()

        pattern = f"{self.tablename}:*"
        all_keys = await client.keys(pattern)

        main_keys = []
        for key in all_keys:
            main_key = key[len(self.tablename) + 1 :]
            main_keys.append(main_key)

        return main_keys

    async def set_expire(self, key: str, ttl: int) -> int:
        key_path = self.get_key_path(key)
        client = self._get_client()
        return await client.expire(key_path, ttl)

    async def get_expire(self, key: str) -> int | None:
        key_path = self.get_key_path(key)
        client = self._get_client()

        # Inside pipeline, pipe doesn't return data
        if self._transaction_pipeline is not None:
            return None

        ttl = await client.ttl(key_path)

        if ttl == -2:
            return None
        elif ttl == -1:
            return None
        else:
            return max(0, ttl)

    async def delete_expired_attributes(self):
        return

    def get_datetime_format(self) -> str:
        return "%Y-%m-%d %H:%M:%S"

    async def begin(self):
        if self._transaction_pipeline is not None:
            raise RuntimeError("Transaction already in progress")

        self._transaction_pipeline = self._client.pipeline(transaction=True)

    async def commit(self):
        if self._transaction_pipeline is None:
            raise RuntimeError("No transaction in progress")

        try:
            # Execute all queued commands atomically
            results = await self._transaction_pipeline.execute()
            return results
        finally:
            # Clean up transaction state
            self._transaction_pipeline = None

    async def rollback(self):
        """Rollback transaction by discarding the pipeline."""
        if self._transaction_pipeline is None:
            raise RuntimeError("No transaction in progress")

        await self._transaction_pipeline.reset()
        self._transaction_pipeline = None

    def get_support_transactions(self) -> bool:
        return True

    def get_support_for_streams(self) -> bool:
        return True

    def count_expired_keys(self) -> int:
        return

    def get_all_keys_with_expiry(self):
        return

    async def get_all_keys_with_expiry_async(self):
        return

    def delete_expired_attributes(self):
        return

    def get_support_datatype_serializer(self):
        return False
