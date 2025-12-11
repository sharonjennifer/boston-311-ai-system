import os 
import hashlib
import logging
import time

from dotenv import load_dotenv
from collections import OrderedDict

load_dotenv()

LOG_LEVEL = os.getenv("B311_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b311.cache")


class QueryCache:

    def __init__(self, max_size = 100, ttl_seconds = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "expirations": 0
        }
        logger.info(f"Initialized QueryCache (max_size={max_size}, ttl={ttl_seconds}s)")
    
    def get_cache_key(self, sql_query):
        normalized = " ".join(sql_query.split())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def is_expired(self, entry):
        age = time.time() - entry["timestamp"]
        return age > self.ttl_seconds
    
    def get(self, sql_query):
        cache_key = self.get_cache_key(sql_query)
        
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            
            if self.is_expired(entry):
                logger.info(f"Cache EXPIRED for query: {cache_key[:8]}...")
                del self.cache[cache_key]
                self.stats["expirations"] += 1
                self.stats["misses"] += 1
                return None
            
            self.cache.move_to_end(cache_key)
            
            self.stats["hits"] += 1
            hit_rate = self.stats["hits"] / (self.stats["hits"] + self.stats["misses"]) * 100
            logger.info(f"Cache HIT for query: {cache_key[:8]}... (hit rate: {hit_rate:.1f}%)")
            
            return entry["records"]
        
        self.stats["misses"] += 1
        logger.info(f"Cache MISS for query: {cache_key[:8]}...")
        return None
    
    def set(self, sql_query, records):
        cache_key = self.get_cache_key(sql_query)
        
        if len(self.cache) >= self.max_size and cache_key not in self.cache:
            evicted_key = next(iter(self.cache))
            del self.cache[evicted_key]
            self.stats["evictions"] += 1
            logger.debug(f"Cache FULL - evicted oldest entry")
        
        self.cache[cache_key] = {
            "records": records,
            "timestamp": time.time(),
            "sql": sql_query[:100]
        }
        
        logger.info(f"Cache SET for query: {cache_key[:8]}... (cache size: {len(self.cache)}/{self.max_size})")
    
    def invalidate(self, sql_query):
        cache_key = self.get_cache_key(sql_query)
        if cache_key in self.cache:
            del self.cache[cache_key]
            logger.info(f"Cache INVALIDATED for query: {cache_key[:8]}...")
    
    def clear(self):
        old_size = len(self.cache)
        self.cache.clear()
        logger.info(f"Cache CLEARED - removed {old_size} entries")
    
    def get_stats(self):
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
    
    def get_cached_queries(self):
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
    
query_cache = None

def get_query_cache():
    global query_cache
    if query_cache is None:
        query_cache = QueryCache(max_size=100, ttl_seconds=3600)
    return query_cache