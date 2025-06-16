"""
Cache manager per Query Service.
"""
import json
import hashlib
import logging
from typing import Any, Dict, Optional

from src.cache.redis_cache import RedisCache
from src.cache.memory_cache import MemoryCache
from src.configg import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    CACHE_ENABLED, CACHE_TTL
)

logger = logging.getLogger(__name__)

class QueryCacheManager:
    """Gestisce cache per query results."""
    
    def __init__(self):
        if CACHE_ENABLED:
            try:
                self.cache = RedisCache(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD if REDIS_PASSWORD else None,
                    default_ttl=CACHE_TTL
                )
                if self.cache.client is None:
                    logger.warning("Redis non disponibile, uso memory cache")
                    self.cache = MemoryCache(default_ttl=CACHE_TTL)
            except Exception as e:
                logger.error(f"Errore init Redis cache: {e}")
                self.cache = MemoryCache(default_ttl=CACHE_TTL)
        else:
            self.cache = None
            
    def get_cache_key(self, query_type: str, params: Dict[str, Any]) -> str:
        """Genera cache key univoca per query."""
        # Serializza parametri in modo deterministico
        params_str = json.dumps(params, sort_keys=True, default=str)
        hash_str = hashlib.md5(params_str.encode()).hexdigest()
        return f"query:{query_type}:{hash_str}"
        
    async def get(self, key: str) -> Optional[Any]:
        """Recupera da cache."""
        if not self.cache:
            return None
            
        try:
            return self.cache.get(key)
        except Exception as e:
            logger.error(f"Errore get cache: {e}")
            return None
            
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Salva in cache."""
        if not self.cache:
            return False
            
        try:
            return self.cache.set(key, value, ttl)
        except Exception as e:
            logger.error(f"Errore set cache: {e}")
            return False