# PyCache

A powerful Python caching library with TTL support, multiple backend storage options, sharding capabilities, and SQL query building features.

## Features

- **Multiple Backend Support**: In-memory, SQLite, and MySQL storage backends
- **TTL (Time-To-Live) Support**: Automatic expiration of cached items
- **Sharding Support**: Distribute cache across multiple database instances
- **Batch Operations**: Efficient bulk get/set operations
- **Thread-Safe**: Designed for concurrent access

# knowledge dump

### Fighting with Concurrency
* Instead of sharing common state in concurrent environment which I did and fought to safeguard a shared state, I came with this Factory + session based approach(see here what was before - https://github.com/ArnabChatterjee20k/pycache/commit/fd04ec6ec4643fe7f317529a138bab79cf21ef08)

* Each sessions represent the actual connectivity to the database

* Pycache represents a factory for starting db connectivity and ttl worker

* As a result I can extend the session creation functionality to implement pooling as well
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