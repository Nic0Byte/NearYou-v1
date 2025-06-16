"""Operatori custom per Bytewax dataflow."""
import asyncio
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import asyncpg
import httpx
from clickhouse_driver import Client as CHClient

from src.configg import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    MESSAGE_GENERATOR_URL,
)

logger = logging.getLogger(__name__)

# Soglia distanza per messaggi
MAX_POI_DISTANCE = 200  # metri

class DatabaseConnections:
    """Gestisce connessioni database con pattern singleton e pooling."""
    
    def __init__(self):
        self._pg_pool = None
        self._ch_client = None
        self._http_client = None
        self._loop = None
        self._message_cache = {}  # Cache semplice in-memory
        
    @property
    def loop(self):
        """Get or create event loop."""
        if self._loop is None or self._loop.is_closed():
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop
        
    async def get_pg_pool(self) -> asyncpg.Pool:
        """Ottieni pool PostgreSQL (lazy init)."""
        if self._pg_pool is None:
            self._pg_pool = await asyncpg.create_pool(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
                min_size=2, max_size=10,
                command_timeout=10
            )
        return self._pg_pool
        
    def get_ch_client(self) -> CHClient:
        """Ottieni client ClickHouse (lazy init)."""
        if self._ch_client is None:
            self._ch_client = CHClient(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
                send_receive_timeout=10
            )
        return self._ch_client
        
    async def get_http_client(self) -> httpx.AsyncClient:
        """Ottieni client HTTP (lazy init)."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client
        
    def get_cache_key(self, user_id: int, shop_id: int) -> str:
        """Genera chiave cache per messaggi."""
        return f"{user_id}:{shop_id}"
        
    async def close(self):
        """Chiudi tutte le connessioni."""
        if self._pg_pool:
            await self._pg_pool.close()
        if self._http_client:
            await self._http_client.aclose()

# Funzioni helper asincrone
async def _find_nearest_shop(conn: DatabaseConnections, lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """Trova il negozio più vicino usando PostGIS."""
    try:
        pool = await conn.get_pg_pool()
        row = await pool.fetchrow(
            """
            SELECT
              shop_id,
              shop_name,
              category,
              ST_Distance(
                geom::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
              ) AS distance
            FROM shops
            ORDER BY distance
            LIMIT 1
            """,
            lon, lat
        )
        
        if row:
            return {
                "shop_id": row["shop_id"],
                "shop_name": row["shop_name"],
                "category": row["category"],
                "distance": row["distance"]
            }
        return None
    except Exception as e:
        logger.error(f"Errore query PostGIS: {e}")
        return None

async def _get_user_profile(conn: DatabaseConnections, user_id: int) -> Optional[Dict[str, Any]]:
    """Recupera profilo utente da ClickHouse."""
    try:
        ch = conn.get_ch_client()
        result = ch.execute(
            """
            SELECT user_id, age, profession, interests
            FROM users
            WHERE user_id = %(user_id)s
            LIMIT 1
            """,
            {"user_id": user_id}
        )
        if result:
            return {
                "user_id": result[0][0],
                "age": result[0][1],
                "profession": result[0][2],
                "interests": result[0][3]
            }
        return None
    except Exception as e:
        logger.error(f"Errore recupero profilo utente {user_id}: {e}")
        return None

async def _generate_message(conn: DatabaseConnections, user: Dict, shop: Dict) -> str:
    """Genera messaggio personalizzato via API."""
    try:
        # Check cache
        cache_key = conn.get_cache_key(user["user_id"], shop["shop_id"])
        if cache_key in conn._message_cache:
            logger.debug(f"Cache hit per {cache_key}")
            return conn._message_cache[cache_key]
            
        # Call API
        client = await conn.get_http_client()
        payload = {
            "user": {
                "age": user["age"],
                "profession": user["profession"],
                "interests": user["interests"]
            },
            "poi": {
                "name": shop["shop_name"],
                "category": shop["category"],
                "description": f"Negozio a {shop['distance']:.0f}m di distanza"
            }
        }
        
        response = await client.post(MESSAGE_GENERATOR_URL, json=payload)
        if response.status_code == 200:
            message = response.json()["message"]
            # Cache result
            conn._message_cache[cache_key] = message
            return message
        else:
            logger.error(f"Errore API: {response.status_code}")
            return ""
    except Exception as e:
        logger.error(f"Errore generazione messaggio: {e}")
        return ""

# Operatori Bytewax
def enrich_with_nearest_shop(item: Tuple[str, Dict], conn: DatabaseConnections) -> List[Tuple[str, Dict]]:
    """Arricchisce evento con negozio più vicino."""
    key, event = item
    
    # Esegui query asincrona in modo sincrono
    loop = conn.loop
    shop = loop.run_until_complete(
        _find_nearest_shop(conn, event["latitude"], event["longitude"])
    )
    
    if shop:
        # Merge shop data into event
        event.update(shop)
        return [(key, event)]
    else:
        logger.warning(f"Nessun negozio trovato per user {key}")
        return []

def check_proximity_and_generate_message(item: Tuple[str, Dict], conn: DatabaseConnections) -> List[Tuple[str, Dict]]:
    """Genera messaggio se utente è in prossimità."""
    key, event = item
    
    # Check distanza
    distance = event.get("distance", float('inf'))
    if distance > MAX_POI_DISTANCE:
        # Troppo lontano, passa evento senza messaggio
        event["poi_info"] = ""
        return [(key, event)]
    
    # Recupera profilo e genera messaggio
    loop = conn.loop
    user_profile = loop.run_until_complete(
        _get_user_profile(conn, int(key))
    )
    
    if user_profile:
        message = loop.run_until_complete(
            _generate_message(conn, user_profile, event)
        )
        event["poi_info"] = message
    else:
        event["poi_info"] = ""
        
    return [(key, event)]

def write_to_clickhouse(item: Tuple[str, Dict], conn: DatabaseConnections) -> None:
    """Scrive evento in ClickHouse."""
    key, event = item
    
    try:
        ch = conn.get_ch_client()
        
        # Parse timestamp
        ts = datetime.fromisoformat(event["timestamp"]).astimezone(timezone.utc).replace(tzinfo=None)
        
        # Insert
        ch.execute(
            """
            INSERT INTO user_events
              (event_id, event_time, user_id, latitude, longitude, 
               poi_range, poi_name, poi_info)
            VALUES
            """,
            [(
                event.get("_offset", 0),
                ts,
                int(key),
                event["latitude"],
                event["longitude"],
                event.get("distance", 0),
                event.get("shop_name", ""),
                event.get("poi_info", "")
            )]
        )
        
        if event.get("poi_info"):
            logger.info(f" Evento con messaggio salvato per user {key}")
    except Exception as e:
        logger.error(f"Errore scrittura ClickHouse: {e}")