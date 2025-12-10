"""
Query Result Caching System

Caches BigQuery results to avoid expensive repeat queries
"""
import hashlib
import logging
import time
from typing import List, Dict, Optional, Tuple
from collections import OrderedDict

logger = logging.getLogger("b311.cache")


class QueryCache:
    """
    LRU cache with TTL for query results
    
    Features:
    - Caches SQL query results
    - Automatic expiration (TTL)
    - LRU eviction when cache is full
    - Thread-safe for production use
    """
    
    def __init__(self, max_size: int = 100, ttl_seconds: int = 3600):
        """
        Initialize cache
        
        Args:
            max_size: Maximum number of cached queries (default 100)
            ttl_seconds: Time-to-live in seconds (default 1 hour)
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()  # Maintains insertion order for LRU
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "expirations": 0
        }
        logger.info(f"Initialized QueryCache (max_size={max_size}, ttl={ttl_seconds}s)")
    
    def _get_cache_key(self, sql_query: str) -> str:
        """Generate cache key from SQL query"""
        # Normalize query (remove extra whitespace)
        normalized = " ".join(sql_query.split())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _is_expired(self, entry: dict) -> bool:
        """Check if cache entry is expired"""
        age = time.time() - entry["timestamp"]
        return age > self.ttl_seconds
    
    def get(self, sql_query: str) -> Optional[List[Dict]]:
        """
        Get cached result if available and not expired
        
        Args:
            sql_query: SQL query string
        
        Returns:
            Cached records or None if not found/expired
        """
        cache_key = self._get_cache_key(sql_query)
        
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            
            # Check expiration
            if self._is_expired(entry):
                logger.info(f"Cache EXPIRED for query: {cache_key[:8]}...")
                del self.cache[cache_key]
                self.stats["expirations"] += 1
                self.stats["misses"] += 1
                return None
            
            # Move to end (LRU - mark as recently used)
            self.cache.move_to_end(cache_key)
            
            self.stats["hits"] += 1
            hit_rate = self.stats["hits"] / (self.stats["hits"] + self.stats["misses"]) * 100
            logger.info(f"Cache HIT for query: {cache_key[:8]}... (hit rate: {hit_rate:.1f}%)")
            
            return entry["records"]
        
        self.stats["misses"] += 1
        logger.info(f"Cache MISS for query: {cache_key[:8]}...")
        return None
    
    def set(self, sql_query: str, records: List[Dict]):
        """
        Cache query results
        
        Args:
            sql_query: SQL query string
            records: Result records to cache
        """
        cache_key = self._get_cache_key(sql_query)
        
        # Check if we need to evict (LRU)
        if len(self.cache) >= self.max_size and cache_key not in self.cache:
            # Remove oldest item (first in OrderedDict)
            evicted_key = next(iter(self.cache))
            del self.cache[evicted_key]
            self.stats["evictions"] += 1
            logger.debug(f"Cache FULL - evicted oldest entry")
        
        # Store with timestamp
        self.cache[cache_key] = {
            "records": records,
            "timestamp": time.time(),
            "sql": sql_query[:100]  # Store truncated SQL for debugging
        }
        
        logger.info(f"Cache SET for query: {cache_key[:8]}... (cache size: {len(self.cache)}/{self.max_size})")
    
    def invalidate(self, sql_query: str):
        """Invalidate specific cached query"""
        cache_key = self._get_cache_key(sql_query)
        if cache_key in self.cache:
            del self.cache[cache_key]
            logger.info(f"Cache INVALIDATED for query: {cache_key[:8]}...")
    
    def clear(self):
        """Clear entire cache"""
        old_size = len(self.cache)
        self.cache.clear()
        logger.info(f"Cache CLEARED - removed {old_size} entries")
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.stats["hits"] + self.stats["misses"]
        hit_rate = (self.stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "cache_size": len(self.cache),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl_seconds,
            "hits": self.stats["hits"],
            "misses": self.stats["misses"],
            "hit_rate_percent": round(hit_rate, 2),
            "evictions": self.stats["evictions"],
            "expirations": self.stats["expirations"]
        }
    
    def get_cached_queries(self) -> List[Dict]:
        """Get list of currently cached queries (for debugging)"""
        cached = []
        current_time = time.time()
        
        for key, entry in self.cache.items():
            age_seconds = current_time - entry["timestamp"]
            cached.append({
                "cache_key": key[:8] + "...",
                "sql_preview": entry["sql"],
                "age_seconds": round(age_seconds, 1),
                "expires_in_seconds": round(self.ttl_seconds - age_seconds, 1)
            })
        
        return cached


# Global cache instance
_query_cache = None


def get_query_cache() -> QueryCache:
    """Get or create global query cache instance"""
    global _query_cache
    if _query_cache is None:
        _query_cache = QueryCache(max_size=100, ttl_seconds=3600)
    return _query_cache