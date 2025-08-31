# PyCache Adapter Comparison

This document provides a comprehensive comparison of PyCache adapters, their supported features, and implementation status.

## Core Features Comparison

| Feature | InMemory | SQLite | Redis |
|---------|----------|---------|-------|
| **Status** | ✅ Stable | ✅ Stable | 🧪 Experimental |
| **Storage Type** | In-Memory | File-based | Network |
| **Concurrency** | Thread-safe with locks | Single-threaded with queue | Async with connection pool |
| **Persistence** | ✅ Custom Snapshots | ✅ Built-in Database | ✅ Built-in Persistence |
| **Network** | ❌ No | ❌ No | ✅ Yes |
| **Snapshot Support** | ✅ Custom System | ✅ Database Persistence | ✅ Redis RDB/AOF |

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
| **Snapshot Management** | ✅ Custom | ✅ Database | ✅ Redis Native | Different persistence approaches |

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
- **Snapshot Support**: Custom snapshot system with configurable triggers and file-based persistence

### SQLite Adapter
- **Storage**: SQLite database with custom schema
- **Serialization**: Pickle-based serialization
- **TTL**: Database-level expiration tracking
- **Transactions**: Full ACID transaction support
- **Performance**: Good for persistent storage, single-threaded
- **Snapshot Support**: Built-in database persistence with ACID guarantees

### Redis Adapter (Experimental)
- **Storage**: Redis server with native data structures
- **Serialization**: No serialization - native Redis types
- **TTL**: Native Redis expiration
- **Transactions**: Redis MULTI/EXEC support
- **Performance**: Network-based, supports clustering
- **Special Features**: Pipeline operations, mixed datatype batch operations
- **Snapshot Support**: Built-in Redis persistence (RDB snapshots, AOF logging)

## Snapshot Support Matrix

### Snapshot Operations

| Method | InMemory | SQLite | Redis | Notes |
|--------|----------|---------|-------|-------|
| `enable_snapshots(config)` | ✅ | ❌ | ❌ | Enable custom snapshot functionality |
| `disable_snapshots()` | ✅ | ❌ | ❌ | Disable custom snapshot functionality |
| `snapshots_enabled` | ✅ | ❌ | ❌ | Property to check custom snapshot status |
| `force_snapshot()` | ✅ | ❌ | ❌ | Manually trigger custom snapshot creation |
| `load_snapshot(timestamp)` | ✅ | ❌ | ❌ | Load custom snapshot from specific timestamp |
| `prune_old_snapshots()` | ✅ | ❌ | ❌ | Clean up old custom snapshot files |
| **Built-in Persistence** | ❌ | ✅ | ✅ | Database/Redis native persistence |

### Snapshot Configuration

| Configuration | InMemory | SQLite | Redis | Default Value |
|---------------|----------|---------|-------|---------------|
| `dir` | ✅ | ❌ | ❌ | `"./snapshot"` |
| `min_changes` | ✅ | ❌ | ❌ | `100` |
| `interval_hours` | ✅ | ❌ | ❌ | `1` |
| `auto` | ✅ | ❌ | ❌ | `False` |
| `max_snapshots` | ✅ | ❌ | ❌ | `4` |

### Persistence Mechanisms

| Adapter | Persistence Type | Implementation | Configuration |
|---------|------------------|----------------|---------------|
| **InMemory** | Custom Snapshots | File-based binary format | Configurable triggers and retention |
| **SQLite** | Database Persistence | ACID transactions, WAL mode | Database file location |
| **Redis** | Native Persistence | RDB snapshots, AOF logging | Redis server configuration |

## Snapshot Specifications

### InMemory Custom Snapshot System
PyCache snapshots use a custom binary format optimized for Python data structures with the following specifications:

#### Header Structure
- **Key Count**: Variable-length encoded integer indicating total number of keys
- **EOF Marker**: Single byte (0x00) marking end of file

#### Data Encoding
- **Type Identification**: Single byte enum for each data type
- **Length Encoding**: Variable-length encoding supporting three ranges:
  - 6-bit: 0x00 to 0x3F (0-63)
  - 14-bit: 0x40 to 0x7F (64-16,383) 
  - 32-bit: 0x80+ (16,384+)
- **Value Encoding**: Type-specific serialization with compression support

#### Supported Data Types
| Type ID | Python Type | Description |
|----------|-------------|-------------|
| 1 | `str` | String data with UTF-8 encoding |
| 2 | `dict` | Key-value mappings |
| 3 | `list` | Sequential collections |
| 4 | `int` | Integer values |
| 5 | `float` | Floating-point values |
| 6 | `set` | Unique collections |
| 7 | `deque` | Double-ended queues |
| 8 | `datetime` | ISO format datetime strings |
| 9 | `None` | Null values |
| 10 | `bool` | Boolean values (stored as 0/1) |

#### Compression Support
- **LZF Compression**: Automatic compression for large string values
- **Encoding Markers**: 0xC3 prefix for compressed data
- **Fallback**: Uncompressed storage for small values

#### File Naming Convention
- **Format**: `YYYY-MM-DD_HH-MM-SS-ffffff`
- **Uniqueness**: Automatic counter suffix for timestamp collisions
- **Example**: `2024-01-15_14-30-25-123456_1.snapshot`

### Snapshot Triggers
Snapshots are automatically created based on two conditions:

1. **Change Threshold**: When `min_changes` operations are performed
2. **Time Interval**: When `interval_hours` have passed since last snapshot

### Automatic Cleanup
- **Retention Policy**: Keeps only `max_snapshots` most recent files
- **Cleanup Strategy**: Removes oldest snapshots based on modification time
- **Error Handling**: Graceful failure handling during cleanup operations

### Performance Characteristics
- **Write Performance**: Optimized binary format with minimal overhead
- **Read Performance**: Fast loading with type-aware deserialization
- **Storage Efficiency**: Automatic compression and efficient encoding
- **Memory Usage**: Minimal memory footprint during snapshot operations

### Thread Safety
- **Multiprocessing Support**: Background worker process for automatic snapshots
- **Lock Management**: Thread-safe change counting and snapshot creation
- **Process Isolation**: Snapshot operations don't block main cache operations