from .py_cache import PyCache
from .adapters.Adapter import Adapter
from .adapters.InMemory import InMemory
from .datatypes.Datatype import Datatype
from .datatypes.String import String
from .datatypes.Numeric import Numeric
from functools import wraps
import asyncio
import pickle
from inspect import iscoroutinefunction
import hashlib


def get_hash_key(prefix, fn, *args, **kwargs):
    raw_key = f"{fn.__name__}.{fn.__qualname__}:{args}:{kwargs}"
    hashed = hashlib.sha256(raw_key.encode()).hexdigest()
    return f"{prefix}:{hashed}"


def cache(ttl=-1, adapter=InMemory(), return_type=String):
    if not isinstance(adapter, Adapter):
        raise TypeError("adapter must be a instance of Adapter")

    if not issubclass(return_type, Datatype):
        raise TypeError("return_type must be a subclass of Datatype")

    pycache = PyCache(adapter)

    def decorator(fn):
        async def get_or_set_cache(*args, **kwargs):
            key = get_hash_key("cache_result", fn, *args, **kwargs)
            async with pycache.session() as session:
                value = await session.get(key)
                if value:
                    return value

                result = (
                    await fn(*args, **kwargs)
                    if iscoroutinefunction(fn)
                    else fn(*args, **kwargs)
                )

                await session.set(key, return_type(result))
                if ttl > 0:
                    await session.set_expire(key, ttl)

                return result

        if iscoroutinefunction(fn):

            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                return await get_or_set_cache(*args, **kwargs)

            return async_wrapper
        else:

            @wraps(fn)
            def sync_wrapper(*args, **kwargs):
                try:
                    loop = asyncio.get_running_loop()
                    future = asyncio.ensure_future(get_or_set_cache(*args, **kwargs))
                    return loop.run_until_complete(future)
                except RuntimeError:
                    return asyncio.run(get_or_set_cache(*args, **kwargs))

            return sync_wrapper

    return decorator


def rate_limit(
    limit=-1, ttl=-1, exception=Exception("Rate limit exceeded"), adapter=InMemory()
):
    if not isinstance(adapter, Adapter):
        raise TypeError("adapter must be a instance of Adapter")

    pycache = PyCache(adapter)

    def decorator(fn):
        async def limit_check_and_call(*args, **kwargs):
            if limit == -1:
                return (
                    await fn(*args, **kwargs)
                    if iscoroutinefunction(fn)
                    else fn(*args, **kwargs)
                )
            key = get_hash_key("rate_limit", fn, *args, **kwargs)

            async with pycache.session() as session:
                value = await session.get(key)
                count = int(value) if value else 0

                if count >= limit:
                    raise exception

                await session.set(key, Numeric(count + 1))
                if ttl > 0:
                    await session.set_expire(key, ttl)

            result = (
                await fn(*args, **kwargs)
                if iscoroutinefunction(fn)
                else fn(*args, **kwargs)
            )
            return result

        if iscoroutinefunction(fn):

            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                return await limit_check_and_call(*args, **kwargs)

            return async_wrapper
        else:

            @wraps(fn)
            def sync_wrapper(*args, **kwargs):
                try:
                    loop = asyncio.get_running_loop()
                    future = asyncio.ensure_future(
                        limit_check_and_call(*args, **kwargs)
                    )
                    return loop.run_until_complete(future)
                except RuntimeError:
                    return asyncio.run(limit_check_and_call(*args, **kwargs))

            return sync_wrapper

    return decorator
