import os
import hashlib
import logging
import sys
from typing import Dict, Any, Optional

# Aggiungi il percorso principale al PYTHONPATH
sys.path.insert(0, '/workspace')  # Percorso alla radice del progetto

try:
    # Tentativo di importazione diretta dalla nuova directory
    from src.cache.redis_cache import RedisCache
    from src.cache.memory_cache import MemoryCache
    logging.info("Moduli di cache importati con successo")
except ImportError as e:
    logging.warning(f"Errore importazione moduli cache: {e}")
    
    # Fallback: importazione con percorso assoluto
    try:
        import importlib.util
        
        # Percorsi aggiornati alla nuova directory
        redis_path = '/workspace/src/cache/redis_cache.py'
        memory_path = '/workspace/src/cache/memory_cache.py'
        
        # Importa RedisCache
        spec = importlib.util.spec_from_file_location("redis_cache_module", redis_path)
        redis_cache_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(redis_cache_module)
        RedisCache = redis_cache_module.RedisCache
        
        # Importa MemoryCache
        spec = importlib.util.spec_from_file_location("memory_cache_module", memory_path)
        memory_cache_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(memory_cache_module)
        MemoryCache = memory_cache_module.MemoryCache
        
        logging.info("Moduli di cache importati tramite percorso assoluto")
    except Exception as fallback_error:
        logging.error(f"Errore importazione moduli cache (fallback): {fallback_error}")
        
        # Definisci classi fallback minime
        class RedisCache:
            def __init__(self, **kwargs):
                self.client = None
                logging.warning("Usando implementazione fallback di RedisCache")
            
            def get(self, key): return None
            def set(self, key, value, ttl=None): return False
            def info(self): return {"status": "fallback-implementation"}
            
        class MemoryCache:
            def __init__(self, **kwargs):
                self.cache = {}
                logging.warning("Usando implementazione fallback di MemoryCache")
            
            def get(self, key): return self.cache.get(key)
            def set(self, key, value, ttl=None): 
                self.cache[key] = value
                return True
            def info(self): 
                return {"status": "fallback-memory", "total_keys": len(self.cache)}

logger = logging.getLogger(__name__)

# Configurazione dalla cache
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))  # 24 ore default
REDIS_HOST = os.getenv("REDIS_HOST", "redis-cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Gestione corretta della password: None se vuota o non presente
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
if not REDIS_PASSWORD:
    REDIS_PASSWORD = None
    logger.info("Redis senza password configurata")
else:
    logger.info(f"Redis con password configurata (lunghezza: {len(REDIS_PASSWORD)})")

# Statistiche cache
cache_stats = {
    "hits": 0,
    "misses": 0,
    "total": 0
}

# Inizializza cache
try:
    if CACHE_ENABLED:
        logger.info(f"Tentativo connessione Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB} (auth: {'yes' if REDIS_PASSWORD else 'no'})")
        
        # Inizializza RedisCache con o senza password in base alla configurazione
        if REDIS_PASSWORD is None:
            cache = RedisCache(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                default_ttl=CACHE_TTL
            )
        else:
            cache = RedisCache(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                default_ttl=CACHE_TTL
            )
            
        # Fallback a MemoryCache se Redis non disponibile
        if cache.client is None:
            logger.warning(f"Redis non disponibile su {REDIS_HOST}:{REDIS_PORT}, usando cache in-memory")
            cache = MemoryCache(default_ttl=CACHE_TTL)
        else:
            logger.info(f"Connessione Redis stabilita: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    else:
        logger.info("Cache disabilitata da configurazione")
        cache = None
except Exception as e:
    logger.warning(f"Errore inizializzazione cache: {e}, usando cache in-memory")
    cache = MemoryCache(default_ttl=CACHE_TTL)

def generate_cache_key(user_params: Dict[str, Any], poi_params: Dict[str, Any]) -> str:
    """
    Genera una chiave di cache basata sui parametri dell'utente e del POI.
    Usa una strategia di fuzzy matching per aumentare gli hit di cache.
    """
    # Normalizza età a range di 5 anni per aumentare la possibilità di cache hit
    age = user_params.get("age", 0)
    age_range = f"{(age // 5) * 5}-{((age // 5) * 5) + 4}"
    
    # Riordina gli interessi alfabeticamente 
    interests = user_params.get("interests", "")
    interests_list = sorted([i.strip().lower() for i in interests.split(",") if i.strip()])
    normalized_interests = ",".join(interests_list)
    
    # Normalizza professione (minuscolo)
    profession = user_params.get("profession", "").lower()
    
    # Combina parametri poi
    poi_name = poi_params.get("name", "").lower()
    poi_category = poi_params.get("category", "").lower()
    
    # Crea stringa combinata e genera hash MD5
    combined = f"{age_range}:{profession}:{normalized_interests}:{poi_name}:{poi_category}"
    hash_key = hashlib.md5(combined.encode()).hexdigest()
    
    # Log per debug
    logger.info(f"CACHE_KEY INPUTS: age={age}, profession={profession}")
    logger.info(f"CACHE_KEY NORMALIZED: age_range={age_range}, normalized_interests={normalized_interests}")
    logger.info(f"CACHE_KEY COMBINED: {combined}")
    logger.info(f"CACHE_KEY HASH: {hash_key}")
    
    return hash_key

def get_cached_message(user_params: Dict[str, Any], poi_params: Dict[str, Any]) -> Optional[str]:
    """Recupera un messaggio dalla cache se disponibile."""
    if not cache or not CACHE_ENABLED:
        return None
        
    cache_key = generate_cache_key(user_params, poi_params)
    result = cache.get(cache_key)
    
    # Aggiorna statistiche
    cache_stats["total"] += 1
    if result:
        cache_stats["hits"] += 1
        logger.debug(f"Cache HIT: {cache_key}")
    else:
        cache_stats["misses"] += 1
        logger.debug(f"Cache MISS: {cache_key}")
        
    return result

def cache_message(user_params: Dict[str, Any], poi_params: Dict[str, Any], message: str) -> bool:
    """Salva un messaggio in cache."""
    if not cache or not CACHE_ENABLED:
        return False
        
    cache_key = generate_cache_key(user_params, poi_params)
    
    # Determina TTL adattivo basato su popolarità categoria
    ttl = CACHE_TTL
    poi_category = poi_params.get("category", "").lower()
    popular_categories = ["ristorante", "bar", "abbigliamento", "supermercato"]
    
    # Messaggi per categorie popolari hanno TTL più lungo
    if poi_category in popular_categories:
        ttl = CACHE_TTL * 2  # TTL doppio per categorie popolari
        
    result = cache.set(cache_key, message, ttl)
    logger.debug(f"Salvato in cache: {cache_key} (TTL={ttl}s)")
    return result

def get_cache_stats():
    """Restituisce statistiche sulla cache."""
    if not cache:
        return {"enabled": False}
        
    hit_rate = cache_stats["hits"] / cache_stats["total"] if cache_stats["total"] > 0 else 0
    
    return {
        "enabled": CACHE_ENABLED,
        "hits": cache_stats["hits"],
        "misses": cache_stats["misses"],
        "total": cache_stats["total"],
        "hit_rate": hit_rate,
        "cache_info": cache.info()
    }