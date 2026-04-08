import os
import pandas as pd
from diskcache import Cache
from typing import Optional, Any
import hashlib
import json

class CacheManager:
    def __init__(self, cache_dir: str = ".scraperfc_cache"):
        self.cache_dir = cache_dir
        self.cache = Cache(cache_dir)

    def _generate_key(self, func_name: str, *args, **kwargs) -> str:
        """ Generates a unique hash key for a specific query. """
        key_data = {
            "func": func_name,
            "args": list(args),
            "kwargs": kwargs
        }
        # Standardize data to ensure same hash for same inputs
        serialized = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.mdsafe(serialized.encode()).hexdigest()

    def get(self, func_name: str, *args, **kwargs) -> Optional[Any]:
        """ Retrieves data from cache if exists. """
        key = self._generate_key(func_name, *args, **kwargs)
        return self.cache.get(key)

    def set(self, data: Any, func_name: str, *args, **kwargs):
        """ Saves data to cache. """
        key = self._generate_key(func_name, *args, **kwargs)
        # Store data. diskcache handles pandas DataFrames and dicts automatically.
        self.cache.set(key, data, expire=60*60*24*7) # Default 1 week expire

    def clear(self):
        """ Clears all cache. """
        self.cache.clear()
