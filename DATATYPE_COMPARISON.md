# PyCache Datatype Comparison

This document provides a comprehensive comparison of all PyCache datatypes, their characteristics, and what Python objects they return.

## Overview

PyCache datatypes are wrapper classes that provide consistent interfaces for different data structures while maintaining type safety and validation. Each datatype inherits from the base `Datatype` class and implements specific validation rules.

## Core Datatypes Comparison

| Datatype | Class | Base Type | Validation | Return Type | Use Case |
|----------|-------|-----------|------------|-------------|----------|
| `String` | `String` | `str` | Converts any value to string | `str` | Text data, serialization |
| `Numeric` | `Numeric` | `int`, `float` | Numeric values only | `int` or `float` | Numbers, calculations |
| `List` | `List` | `Iterable` | Any iterable | `list` | Sequential data, arrays |
| `Map` | `Map` | `dict` | Dictionary only | `dict` | Key-value pairs, objects |
| `Set` | `Set` | `Iterable` | Any iterable | `set` | Unique collections |
| `Queue` | `Queue` | `Iterable` | Any iterable | `collections.deque` | FIFO operations |
| `Streams` | `Streams` | `Iterable` | Any iterable | `list` | Redis streams, ordered data |

## Detailed Datatype Analysis

### String Datatype

**Class**: `String`  
**Base Type**: `str`  
**Validation**: Accepts any value and converts to string  
**Return Type**: `str`  
**Allowed Input**: Any Python object  

```python
from src.pycache.datatypes import String

# Examples
String("hello")           # Returns: "hello"
String(123)               # Returns: "123"
String(True)              # Returns: "True"
String(None)              # Returns: "None"
String({"key": "value"})  # Returns: "{'key': 'value'}"
```

### Numeric Datatype

**Class**: `Numeric`  
**Base Type**: `int`, `float`  
**Validation**: Only accepts numeric values  
**Return Type**: `int` or `float` (preserves original type)  
**Allowed Input**: `int`, `float`  

```python
from src.pycache.datatypes import Numeric

# Examples
Numeric(42)               # Returns: 42 (int)
Numeric(3.14)             # Returns: 3.14 (float)
Numeric(0)                # Returns: 0 (int)
Numeric(-100)             # Returns: -100 (int)

# Invalid inputs (raises TypeError)
# Numeric("123")          # TypeError
# Numeric(True)           # TypeError
# Numeric(None)           # TypeError
```


### List Datatype

**Class**: `List`  
**Base Type**: `Iterable`  
**Validation**: Accepts any iterable object  
**Return Type**: `list`  
**Allowed Input**: `list`, `tuple`, `set`, `str`, `range`, etc.  

```python
from src.pycache.datatypes import List

# Examples
List([1, 2, 3])          # Returns: [1, 2, 3]
List((1, 2, 3))          # Returns: [1, 2, 3]
List({1, 2, 3})          # Returns: [1, 2, 3] (order may vary)
List("hello")             # Returns: ["h", "e", "l", "l", "o"]
List(range(3))            # Returns: [0, 1, 2]
List([])                  # Returns: []

# Invalid inputs (raises TypeError)
# List(123)               # TypeError
# List(None)              # TypeError
```

### Map Datatype

**Class**: `Map`  
**Base Type**: `dict`  
**Validation**: Only accepts dictionaries  
**Return Type**: `dict`  
**Allowed Input**: `dict` only  

```python
from src.pycache.datatypes import Map

# Examples
Map({"name": "John"})     # Returns: {"name": "John"}
Map({})                    # Returns: {}
Map({"nested": {"key": "value"}})  # Returns: {"nested": {"key": "value"}}

# Invalid inputs (raises TypeError)
# Map([1, 2, 3])         # TypeError
# Map("string")           # TypeError
# Map(None)               # TypeError
```

### Set Datatype

**Class**: `Set`  
**Base Type**: `Iterable`  
**Validation**: Accepts any iterable object  
**Return Type**: `set`  
**Allowed Input**: `list`, `tuple`, `set`, `str`, `range`, etc.  

```python
from src.pycache.datatypes import Set

# Examples
Set([1, 2, 3])           # Returns: {1, 2, 3}
Set((1, 2, 3))           # Returns: {1, 2, 3}
Set({1, 2, 3})           # Returns: {1, 2, 3}
Set("hello")              # Returns: {"h", "e", "l", "o"}
Set(range(3))             # Returns: {0, 1, 2}
Set([])                   # Returns: set()

# Invalid inputs (raises TypeError)
# Set(123)                # TypeError
# Set(None)               # TypeError
```

### Queue Datatype

**Class**: `Queue`  
**Base Type**: `Iterable`  
**Validation**: Accepts any iterable object  
**Return Type**: `collections.deque`  
**Allowed Input**: `list`, `tuple`, `set`, `str`, `range`, etc.  

```python
from src.pycache.datatypes import Queue
from collections import deque

# Examples
Queue([1, 2, 3])         # Returns: deque([1, 2, 3])
Queue((1, 2, 3))         # Returns: deque([1, 2, 3])
Queue({1, 2, 3})         # Returns: deque([1, 2, 3]) (order may vary)
Queue("hello")            # Returns: deque(["h", "e", "l", "l", "o"])
Queue(range(3))           # Returns: deque([0, 1, 2])
Queue([])                 # Returns: deque([])

# Invalid inputs (raises TypeError)
# Queue(123)              # TypeError
# Queue(None)             # TypeError
```

### Streams Datatype

**Class**: `Streams`  
**Base Type**: `Iterable`  
**Validation**: Accepts any iterable object  
**Return Type**: `list`  
**Allowed Input**: `list`, `tuple`, `set`, `str`, `range`, `dict`, etc.  

```python
from src.pycache.datatypes import Streams

# Examples
Streams([1, 2, 3])       # Returns: [1, 2, 3]
Streams((1, 2, 3))       # Returns: [1, 2, 3]
Streams({1, 2, 3})       # Returns: [1, 2, 3] (order may vary)
Streams("hello")          # Returns: ["h", "e", "l", "l", "o"]
Streams(range(3))         # Returns: [0, 1, 2]
Streams({"a": 1, "b": 2}) # Returns: [("a", 1), ("b", 2)]
Streams([])               # Returns: []

# Invalid inputs (raises TypeError)
# Streams(123)            # TypeError
# Streams(None)           # TypeError
```

**Characteristics**:
- Most flexible iterable datatype
- Special handling for dictionaries (converts to list of tuples)
- Designed for Redis streams compatibility
- Redis stores as stream data structure

## Validation Rules Summary

| Datatype | Allowed Types | Validation Behavior |
|----------|---------------|-------------------|
| `String` | Any | Converts to string |
| `Numeric` | `int`, `float` | Strict validation |
| `List` | `Iterable` | Converts to list |
| `Map` | `dict` | Strict validation |
| `Set` | `Iterable` | Converts to set |
| `Queue` | `Iterable` | Converts to deque |
| `Streams` | `Iterable` | Converts to list |

## Redis Storage Mapping

| Datatype | Redis Data Structure | Storage Method |
|----------|---------------------|----------------|
| `String` | String | `SET` command |
| `Numeric` | String | `SET` command (stored as string) |
| `List` | List | `LPUSH` command |
| `Map` | Hash | `HSET` command |
| `Set` | Set | `SADD` command |
| `Queue` | List | `LPUSH` command |
| `Streams` | Stream | `XADD` command |