# PyCache

A powerful Python caching library with TTL support, multiple backend storage options, sharding capabilities, and SQL query building features.

## Features

- **Multiple Backend Support**: In-memory, SQLite, and MySQL storage backends
- **TTL (Time-To-Live) Support**: Automatic expiration of cached items
- **Sharding Support**: Distribute cache across multiple database instances
- **Batch Operations**: Efficient bulk get/set operations
- **Thread-Safe**: Designed for concurrent access
- **Single level Transaction Support**: ACID transactions for data consistency
- **Multiple Data Types**: Support for String, List, Map, Numeric, Set, and Queue data types
- **Sharding for Mysql backend**: soon
- **Transactions across multiple store**: soon. Having something like change_tablename or change_store to change the table name and having transaction support across multiple tables in sql
- **Utils**: Common backend-specific decorators like rate limiter, cache functions,etc(great place to contribute)

## Installation

> pip install git+https://github.com/ArnabChatterjee20k/pycache

## Quick Start

### Basic Usage

```python
import asyncio
from src.pycache.py_cache import PyCache
from src.pycache.adapters import InMemory, SQLite
from src.pycache.datatypes import String, List, Map, Numeric, Set, Queue

async def main():
    # Create cache with in-memory backend
    cache = PyCache(InMemory())
    
    # Use cache with session
    async with cache.session() as session:
        # Store different data types
        await session.set("user_name", String("John Doe"))
        await session.set("user_age", Numeric(30))
        await session.set("user_hobbies", List(["reading", "swimming", "coding"]))
        await session.set("user_profile", Map({"city": "New York", "country": "USA"}))
        
        # Retrieve data
        name = await session.get("user_name")
        age = await session.get("user_age")
        hobbies = await session.get("user_hobbies")
        profile = await session.get("user_profile")
        
        print(f"Name: {name}")
        print(f"Age: {age}")
        print(f"Hobbies: {hobbies}")
        print(f"Profile: {profile}")

asyncio.run(main())
```

### Using SQLite Backend

```python
import asyncio
from src.pycache.py_cache import PyCache
from src.pycache.adapters import SQLite
from src.pycache.datatypes import String, List

async def main():
    # Create cache with SQLite backend
    cache = PyCache(SQLite("cache.db"))
    
    async with cache.session() as session:
        # Store data
        await session.set("key1", String("value1"))
        await session.set("key2", List([1, 2, 3, 4, 5]))
        
        # Check if key exists
        if await session.exists("key1"):
            value = await session.get("key1")
            print(f"Value: {value}")
        
        # Get all keys
        all_keys = await session.keys()
        print(f"All keys: {all_keys}")

asyncio.run(main())
```

## Data Types

PyCache supports multiple data types through wrapper classes:

### String
```python
from src.pycache.datatypes import String

await session.set("greeting", String("Hello, World!"))
value = await session.get("greeting")  # Returns "Hello, World!"
```

### Numeric
```python
from src.pycache.datatypes import Numeric

await session.set("count", Numeric(42))
await session.set("price", Numeric(19.99))
value = await session.get("count")  # Returns 42
```

### List
```python
from src.pycache.datatypes import List

await session.set("items", List([1, 2, 3, "hello", True]))
value = await session.get("items")  # Returns [1, 2, 3, "hello", True]
```

### Map (Dictionary)
```python
from src.pycache.datatypes import Map

await session.set("user", Map({"name": "Alice", "age": 25}))
value = await session.get("user")  # Returns {"name": "Alice", "age": 25}
```

### Set
```python
from src.pycache.datatypes import Set

await session.set("unique_items", Set({1, 2, 3, "unique"}))
value = await session.get("unique_items")  # Returns {1, 2, 3, "unique"}
```

### Queue
```python
from src.pycache.datatypes import Queue
from collections import deque

await session.set("task_queue", Queue(deque(["task1", "task2", "task3"])))
value = await session.get("task_queue")  # Returns deque(["task1", "task2", "task3"])
```

## TTL (Time-To-Live) Support

Set expiration time for cached items:

```python
async with cache.session() as session:
    # Store data with 60 seconds TTL
    await session.set("temp_data", String("expires in 60 seconds"))
    await session.set_expire("temp_data", 60)
    
    # Check remaining TTL
    remaining_ttl = await session.get_expire("temp_data")
    print(f"TTL remaining: {remaining_ttl} seconds")
```

### Automatic TTL Cleanup

Start the TTL worker to automatically clean up expired items:

```python
async def main():
    cache = PyCache(SQLite("cache.db"))
    
    # Start TTL worker (runs every 0.5 seconds by default)
    await cache.start_ttl_deletion()
    
    try:
        async with cache.session() as session:
            await session.set("temp_key", String("will expire"))
            await session.set_expire("temp_key", 5)  # Expires in 5 seconds
            
            # Wait for expiration
            await asyncio.sleep(6)
            
            # Key should be automatically removed
            exists = await session.exists("temp_key")
            print(f"Key exists: {exists}")  # False
    finally:
        # Stop TTL worker
        await cache.stop_ttl_deletion()

asyncio.run(main())
```

## Batch Operations

Efficiently handle multiple keys at once:

```python
async with cache.session() as session:
    # Batch set multiple values
    data = {
        "user:1": String("Alice"),
        "user:2": String("Bob"),
        "user:3": String("Charlie"),
        "scores": List([100, 95, 87]),
        "config": Map({"theme": "dark", "lang": "en"})
    }
    
    await session.batch_set(data)
    
    # Batch get multiple values
    results = await session.batch_get(["user:1", "user:2", "scores", "nonexistent"])
    print(results)  # {"user:1": "Alice", "user:2": "Bob", "scores": [100, 95, 87]}
```

## Transactions

Use transactions for data consistency (SQLite backend only):

```python
async with cache.session() as session:
    async with session.with_transaction():
        await session.set("account:1", Numeric(100))
        await session.set("account:2", Numeric(200))
        
        # If any operation fails, all changes are rolled back
        # If all operations succeed, all changes are committed
```

## Advanced Usage

### Key Management

```python
async with cache.session() as session:
    # Store data
    await session.set("key1", String("value1"))
    await session.set("key2", String("value2"))
    
    # Check if key exists
    exists = await session.exists("key1")  # True
    
    # Get all keys
    all_keys = await session.keys()  # ["key1", "key2"]
    
    # Delete specific key
    deleted_count = await session.delete("key1")  # 1
    
    # Check after deletion
    exists = await session.exists("key1")  # False
```

### Error Handling

```python
async with cache.session() as session:
    try:
        # Get non-existent key
        value = await session.get("nonexistent")
        print(f"Value: {value}")  # None
        
        # Set invalid TTL
        await session.set_expire("key", -1)  # Raises ValueError
        
    except ValueError as e:
        print(f"Invalid TTL: {e}")
    except Exception as e:
        print(f"Cache error: {e}")
```

### Concurrent Access

PyCache is designed to handle concurrent access safely:

```python
import asyncio

async def worker(cache, worker_id):
    async with cache.session() as session:
        for i in range(10):
            key = f"worker:{worker_id}:{i}"
            await session.set(key, String(f"data from worker {worker_id}"))
            await asyncio.sleep(0.1)

async def main():
    cache = PyCache(SQLite("concurrent.db"))
    
    # Run multiple workers concurrently
    tasks = [worker(cache, i) for i in range(5)]
    await asyncio.gather(*tasks)
    
    # Verify all data was stored
    async with cache.session() as session:
        keys = await session.keys()
        print(f"Total keys stored: {len(keys)}")

asyncio.run(main())
```

## Backend Comparison

| Feature | InMemory | SQLite |
|---------|----------|--------|
| Persistence | No | Yes |
| Transactions | No | Yes |
| Concurrent Access | Yes | Yes |
| TTL Support | Yes | Yes |
| Performance | Fastest | Fast |
| Memory Usage | High | Low |

## API Reference

### PyCache Class

- `session()`: Create a new cache session
- `start_ttl_deletion(delete_interval=0.5)`: Start automatic TTL cleanup
- `stop_ttl_deletion()`: Stop TTL cleanup
- `delete_expired_attributes()`: Manually delete expired items
- `count_expired_keys()`: Count expired keys
- `get_all_keys_with_expiry()`: Get all keys with their expiry times

### Session Class

- `set(key, value)`: Store a value
- `get(key)`: Retrieve a value
- `delete(key)`: Delete a key
- `exists(key)`: Check if key exists
- `keys()`: Get all keys
- `batch_set(data)`: Store multiple values
- `batch_get(keys)`: Retrieve multiple values
- `set_expire(key, ttl)`: Set TTL for a key
- `get_expire(key)`: Get remaining TTL
- `with_transaction()`: Create a transaction context

### Data Types

- `String(value)`: Store string values
- `Numeric(value)`: Store numeric values (int/float)
- `List(value)`: Store list values
- `Map(value)`: Store dictionary values
- `Set(value)`: Store set values
- `Queue(value)`: Store deque values

## Knowledge Dump

### Session-Based Design

PyCache uses a session-based approach for better concurrency handling

### Fighting with Concurrency
Instead of sharing common state in concurrent environment which I did and fought to safeguard a shared state, I came with this Factory + session based approach

```
PyCache
  ↳ session()
      ↳ creates new SQLiteSession instance
      ↳ returns PyCacheSession(SQLiteSession)

SQLite
  ↳ connect() returns SQLiteSession
SQLiteSession(SQLite)
  ↳ own thread/queue
  ↳ same method interfaces
PyCacheSession(PyCache)
  ↳ own adapter (the SQLiteSession)
  ↳ implements same cache API methods
```

This design allows each session to have its own database connection and thread, preventing shared state issues in concurrent environments.

## Next Steps
1. Instead of a row for list/queue/set -> use tables in case of sqlite
2. Support pagination for list/queue/set
3. Invalidating empty list/queue/set at the Datatype layer(which is getting done at the redis adapter set datatype method)
4. Support for projection of data via datatype(we already have expected_datatype)
5. Stream support for the other adapters
6. Sorted set support for all adapters
7. Vector support
8. Mysql support
9. Sharding support for all adapters
10. Cache eviction
11. Using "TYPE" command of redis to know the types automatically(as an option at the adapter)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request
