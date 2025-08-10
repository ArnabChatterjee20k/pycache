# PyCache Adapter Comparison

This document provides a comprehensive comparison of PyCache adapters, their supported features, and implementation status.

## Core Features Comparison

| Feature | InMemory | SQLite | Redis |
|---------|----------|---------|-------|
| **Status** | ✅ Stable | ✅ Stable | 🧪 Experimental |
| **Storage Type** | In-Memory | File-based | Network |
| **Concurrency** | Thread-safe with locks | Single-threaded with queue | Async with connection pool |
| **Persistence** | ❌ No | ✅ Yes | ✅ Yes |
| **Network** | ❌ No | ❌ No | ✅ Yes |

## Method Support Matrix

### Basic CRUD Operations

| Method | InMemory | SQLite | Redis | Notes |
|--------|----------|---------|-------|-------|
| `connect()` | ✅ | ✅ | ✅ | All adapters support connection management |
| `close()` | ✅ | ✅ | ✅ | All adapters support proper cleanup |
| `create()` | ✅ | ✅ | ✅ | Database/table initialization |
| `create_index()` | ✅ | ✅ | ✅ | Index creation for performance |
| `get(key, datatype)` | ✅ | ✅ | ✅ | Retrieval with datatype support |
| `set(key, value)` | ✅ | ✅ | ✅ | Storage with datatype support |
| `delete(key)` | ✅ | ✅ | ✅ | Key deletion |
| `exists(key)` | ✅ | ✅ | ✅ | Key existence check |
| `keys()` | ✅ | ✅ | ✅ | List all keys |

### Batch Operations

| Method | InMemory | SQLite | Redis | Notes |
|--------|----------|---------|-------|-------|
| `batch_get(keys, datatype)` | ✅ | ✅ | ✅ | Bulk retrieval with optional datatype |
| `batch_set(key_values)` | ✅ | ✅ | ✅ | Bulk storage of multiple key-value pairs |

### TTL/Expiration Support

| Method | InMemory | SQLite | Redis | Notes |
|--------|----------|---------|-------|-------|
| `set_expire(key, ttl)` | ✅ | ✅ | ✅ | Set time-to-live for keys |
| `get_expire(key)` | ✅ | ✅ | ✅ | Get remaining TTL |
| `delete_expired_attributes()` | ✅ | ✅ | ✅ | Clean up expired keys |
| `count_expired_keys()` | ✅ | ✅ | ❌ | Count expired keys |
| `get_all_keys_with_expiry()` | ✅ | ✅ | ❌ | Get keys with expiry info |

### Transaction Support

| Method | InMemory | SQLite | Redis | Notes |
|--------|----------|---------|-------|-------|
| `begin()` | ❌ | ✅ | ✅ | Start transaction |
| `commit()` | ❌ | ✅ | ✅ | Commit transaction |
| `rollback()` | ❌ | ✅ | ✅ | Rollback transaction |
| `get_support_transactions()` | ❌ | ✅ | ✅ | Transaction capability check |

### Advanced Features

| Feature | InMemory | SQLite | Redis | Notes |
|---------|----------|---------|-------|-------|
| **Datatype Serialization** | ✅ | ✅ | ❌ | Redis uses native types |
| **Streams Support** | ❌ | ❌ | ✅ | Redis-specific feature |
| **Pipeline Operations** | ❌ | ❌ | ✅ | Redis performance optimization |
| **Mixed Datatype Batch Get** | ❌ | ❌ | ✅ | Redis-specific feature |
| **Connection Pooling** | ❌ | ❌ | ✅ | Redis connection management |

## Datatype Support

### Core Datatypes (All Adapters)

| Datatype | InMemory | SQLite | Redis | Implementation |
|----------|----------|---------|-------|----------------|
| `String` | ✅ | ✅ | ✅ | Basic string storage |
| `Numeric` | ✅ | ✅ | ✅ | Integer/float storage |
| `List` | ✅ | ✅ | ✅ | Sequential data |
| `Map` | ✅ | ✅ | ✅ | Key-value pairs |
| `Set` | ✅ | ✅ | ✅ | Unique collections |
| `Queue` | ✅ | ✅ | ✅ | FIFO collections |

### Advanced Datatypes

| Datatype | InMemory | SQLite | Redis | Implementation |
|----------|----------|---------|-------|----------------|
| `Streams` | ❌ | ❌ | ✅ | Redis-specific streams |

## Implementation Details

### InMemory Adapter
- **Storage**: Python dictionary with thread-safe locks
- **Serialization**: Direct value storage (no serialization)
- **TTL**: In-memory expiration tracking
- **Transactions**: Not implemented (single-threaded operations)
- **Performance**: Fastest for small datasets, memory-limited

### SQLite Adapter
- **Storage**: SQLite database with custom schema
- **Serialization**: Pickle-based serialization
- **TTL**: Database-level expiration tracking
- **Transactions**: Full ACID transaction support
- **Performance**: Good for persistent storage, single-threaded

### Redis Adapter (Experimental)
- **Storage**: Redis server with native data structures
- **Serialization**: No serialization - native Redis types
- **TTL**: Native Redis expiration
- **Transactions**: Redis MULTI/EXEC support
- **Performance**: Network-based, supports clustering
- **Special Features**: Pipeline operations, mixed datatype batch operations