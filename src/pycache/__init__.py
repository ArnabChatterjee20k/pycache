"""
PyCache - A Python caching library with SQL query building capabilities
"""

from .sql import (
    Composable,
    Composed,
    SQL,
    Identifier,
    Literal,
    Placeholder,
)

from .py_cache import PyCache, Session
from .decorators import cache, rate_limit, get_hash_key

# Import adapters
from .adapters.Adapter import Adapter
from .adapters.InMemory import InMemory
from .adapters.SQLite import SQLite

# Import datatypes
from .datatypes.Datatype import Datatype
from .datatypes.String import String
from .datatypes.Numeric import Numeric
from .datatypes.List import List
from .datatypes.Map import Map
from .datatypes.Set import Set
from .datatypes.Queue import Queue

__version__ = "0.1.0"
__all__ = [
    # SQL components
    "Composable",
    "Composed", 
    "SQL",
    "Identifier",
    "Literal",
    "Placeholder",
    
    # Main classes
    "PyCache",
    "Session",
    
    # Decorators
    "cache",
    "rate_limit", 
    "get_hash_key",
    
    # Adapters
    "Adapter",
    "InMemory",
    "SQLite",
    
    # Datatypes
    "Datatype",
    "String",
    "Numeric",
    "List",
    "Map", 
    "Set",
    "Queue",
]
