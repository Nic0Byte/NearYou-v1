import json
import logging
import redis
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)

class RedisCache:
    """Client Redis per caching con supporto TTL e serializzazione JSON."""
    
    def __init__(
        self, 
        host: str = "localhost", 
        port: int = 6379, 
        db: int = 0, 
        password: Optional[str] = None,
        default_ttl: int = 86400  # 24 ore
    ):
        """Inizializza connessione Redis con parametri configurabili."""
        self.default_ttl = default_ttl
        try:
            # Crea argomenti solo per parametri non nulli
            kwargs = {
                "host": host,
                "port": port,
                "db": db,
                "decode_responses": False,
                "socket_timeout": 5
            }
            
            # Aggiungi password solo se effettivamente presente
            if password is not None and password != "":
                kwargs["password"] = password
                logger.info(f"Connessione a Redis con password (lunghezza: {len(password)})")
            else:
                logger.info("Connessione a Redis senza password")
                
            self.client = redis.Redis(**kwargs)
            
            # Test connessione
            self.client.ping()
            logger.info(f"Cache Redis connessa a {host}:{port}/{db}")
        except Exception as e:
            logger.warning(f"Impossibile connettersi a Redis: {e}")
            self.client = None
    
    def get(self, key: str) -> Optional[Any]:
        """Recupera valore da cache con deserializzazione JSON."""
        if not self.client:
            return None
        
        try:
            value = self.client.get(key)
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    # Se non è JSON, ritorna il valore raw decodificato
                    return value.decode('utf-8')
            return None
        except Exception as e:
            logger.error(f"Errore cache get({key}): {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Salva valore in cache con serializzazione JSON."""
        if not self.client:
            return False
        
        ttl = ttl if ttl is not None else self.default_ttl
        
        try:
            # Serializza oggetti complessi in JSON
            if not isinstance(value, (str, bytes)):
                value = json.dumps(value)
            
            # Converti stringhe in bytes se necessario
            if isinstance(value, str):
                value = value.encode('utf-8')
                
            return self.client.setex(key, ttl, value)
        except Exception as e:
            logger.error(f"Errore cache set({key}): {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Elimina chiave dalla cache."""
        if not self.client:
            return False
            
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            logger.error(f"Errore cache delete({key}): {e}")
            return False
            
    def exists(self, key: str) -> bool:
        """Verifica se la chiave esiste."""
        if not self.client:
            return False
            
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Errore cache exists({key}): {e}")
            return False
    
    def info(self) -> Dict[str, Any]:
        """Restituisce statistiche sul server Redis."""
        if not self.client:
            return {"status": "disconnected"}
            
        try:
            info = self.client.info()
            return {
                "status": "connected",
                "used_memory_human": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "uptime_in_days": info.get("uptime_in_days"),
                "hit_rate": info.get("keyspace_hits", 0) / 
                           (info.get("keyspace_hits", 0) + info.get("keyspace_misses", 1) or 1)
            }
        except Exception as e:
            logger.error(f"Errore cache info(): {e}")
            return {"status": "error", "message": str(e)}