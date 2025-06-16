"""
Business logic per servizi relativi agli utenti.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from clickhouse_driver import Client as CHClient

logger = logging.getLogger(__name__)

class UserService:
    """Service per gestire operazioni relative agli utenti."""
    
    def __init__(self, ch_client: CHClient):
        """Inizializza il service con una connessione ClickHouse."""
        self.ch_client = ch_client
    
    def get_user_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Ottiene il profilo utente dal database."""
        try:
            query = """
                SELECT
                  user_id, age, profession, interests
                FROM users
                WHERE user_id = %(uid)s
                LIMIT 1
            """
            
            rows = self.ch_client.execute(query, {"uid": user_id})
            
            if not rows:
                logger.warning(f"Profilo utente {user_id} non trovato")
                return None
            
            return {
                "user_id": rows[0][0],
                "age": rows[0][1],
                "profession": rows[0][2],
                "interests": rows[0][3]
            }
        except Exception as e:
            logger.error(f"Errore recupero profilo utente {user_id}: {e}")
            return None
    
    def get_recent_positions(self, user_id: int) -> List[Dict[str, Any]]:
        """Ottiene le posizioni più recenti dell'utente."""
        try:
            query = """
                SELECT
                  user_id,
                  argMax(latitude,  event_time) AS lat,
                  argMax(longitude, event_time) AS lon,
                  argMax(poi_info,   event_time) AS msg,
                  max(event_time) as last_time
                FROM user_events
                WHERE user_id = %(uid)s
                GROUP BY user_id
                LIMIT 1
            """
            
            rows = self.ch_client.execute(query, {"uid": user_id})
            
            if not rows:
                return []
            
            r = rows[0]
            return [{
                "user_id": r[0],
                "latitude": r[1],
                "longitude": r[2],
                "message": r[3] or None,
                "timestamp": r[4] if len(r) > 4 else None
            }]
        except Exception as e:
            logger.error(f"Errore recupero posizioni utente {user_id}: {e}")
            return []
    
    def get_promotions(self, user_id: int, limit: int = 10, offset: int = 0) -> List[Dict[str, Any]]:
        """Ottiene le promozioni ricevute dall'utente."""
        try:
            query = """
                SELECT 
                    event_id,
                    event_time,
                    poi_name,
                    poi_info
                FROM user_events
                WHERE user_id = %(uid)s
                  AND poi_info != ''
                ORDER BY event_time DESC
                LIMIT %(limit)s
                OFFSET %(offset)s
            """
            
            rows = self.ch_client.execute(query, {
                "uid": user_id,
                "limit": limit,
                "offset": offset
            })
            
            result = []
            for row in rows:
                result.append({
                    "event_id": row[0],
                    "timestamp": row[1],
                    "shop_name": row[2],
                    "message": row[3]
                })
                
            return result
        except Exception as e:
            logger.error(f"Errore recupero promozioni utente {user_id}: {e}")
            return []