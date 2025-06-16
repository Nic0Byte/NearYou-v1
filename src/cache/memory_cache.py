import time
import threading
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class MemoryCache:
    """Implementazione cache in-memory per sviluppo e testing."""
    
    def __init__(self, default_ttl: int = 86400):
        """Inizializza cache in-memory con pulizia periodica."""
        self.cache = {}  # {key: (value, expire_time)}
        self.default_ttl = default_ttl
        self.lock = threading.RLock()
        
        # Avvia thread di pulizia in background
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_expired_keys, 
            daemon=True
        )
        self.cleanup_thread.start()
    
    def get(self, key: str) -> Optional[Any]:
        """Recupera valore dalla cache."""
        with self.lock:
            if key not in self.cache:
                return None
            
            value, expire_time = self.cache[key]
            
            # Verifica scadenza
            if expire_time is not None and time.time() > expire_time:
                del self.cache[key]
                return None
                
            return value
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Salva valore nella cache."""
        ttl = ttl if ttl is not None else self.default_ttl
        
        with self.lock:
            expire_time = None if ttl is None else time.time() + ttl
            self.cache[key] = (value, expire_time)
            return True
    
    def delete(self, key: str) -> bool:
        """Elimina chiave dalla cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def exists(self, key: str) -> bool:
        """Verifica se la chiave esiste."""
        with self.lock:
            if key not in self.cache:
                return False
                
            _, expire_time = self.cache[key]
            
            # Verifica scadenza
            if expire_time is not None and time.time() > expire_time:
                del self.cache[key]
                return False
                
            return True
    
    def info(self) -> Dict[str, Any]:
        """Restituisce statistiche sulla cache."""
        with self.lock:
            current_time = time.time()
            active_keys = sum(
                1 for _, expire_time in self.cache.values()
                if expire_time is None or expire_time > current_time
            )
            
            return {
                "status": "in-memory",
                "total_keys": len(self.cache),
                "active_keys": active_keys,
                "expired_keys": len(self.cache) - active_keys
            }
    
    def _cleanup_expired_keys(self):
        """Thread di background per la pulizia delle chiavi scadute."""
        while True:
            time.sleep(60)  # Pulizia ogni minuto
            
            with self.lock:
                current_time = time.time()
                keys_to_delete = [
                    key for key, (_, expire_time) in self.cache.items()
                    if expire_time is not None and current_time > expire_time
                ]
                
                for key in keys_to_delete:
                    del self.cache[key]
                
                if keys_to_delete:
                    logger.debug(f"Cache cleanup: rimosse {len(keys_to_delete)} chiavi scadute")