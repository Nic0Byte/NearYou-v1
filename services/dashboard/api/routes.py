"""
Router per le API dashboard utente.
"""
import os
import logging
import httpx
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from clickhouse_driver import Client as CHClient

from .models import (
    UserProfile, PositionsResponse, Position,
    Shop, Promotion, PromotionsResponse, UserStats
)
from .dependencies import get_clickhouse_client, get_current_user

# Aggiungi costante per Query Service
QUERY_SERVICE_URL = os.getenv("QUERY_SERVICE_URL", "http://query-service:8004")

# Database e endpoints sicuri richiedono autenticazione 
router = APIRouter()
logger = logging.getLogger(__name__)

# Helper per chiamate Query Service
async def query_service_request(
    endpoint: str, 
    payload: dict,
    timeout: int = 30
) -> Optional[dict]:
    """Helper per chiamate al Query Service."""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{QUERY_SERVICE_URL}{endpoint}",
                json=payload
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Errore Query Service {endpoint}: {e}")
        return None

@router.get("/profile", response_model=UserProfile)
async def get_user_profile(
    current_user: dict = Depends(get_current_user),
    ch_client: CHClient = Depends(get_clickhouse_client),
    user_id: Optional[int] = Query(None, description="ID dell'utente (solo per debug)")
):
    """Ottiene il profilo dell'utente autenticato."""
    # Per sicurezza, usa l'ID dell'utente corrente, non quello in query
    # a meno che non siamo in modalità debug
    uid = user_id if user_id is not None else current_user["user_id"]
    
    query = """
        SELECT
          user_id, age, profession, interests
        FROM users
        WHERE user_id = %(uid)s
        LIMIT 1
    """
    
    rows = ch_client.execute(query, {"uid": uid})
    
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Profilo utente non trovato"
        )
    
    return {
        "user_id": rows[0][0],
        "age": rows[0][1],
        "profession": rows[0][2],
        "interests": rows[0][3]
    }

@router.get("/positions", response_model=PositionsResponse)
async def get_user_positions(
    current_user: dict = Depends(get_current_user),
    ch_client: CHClient = Depends(get_clickhouse_client)
):
    """Ottiene le posizioni più recenti dell'utente."""
    uid = current_user["user_id"]
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
    rows = ch_client.execute(query, {"uid": uid})
    
    if not rows:
        return {"positions": []}
        
    r = rows[0]
    return {
        "positions": [
            {
                "user_id": r[0],
                "latitude": r[1],
                "longitude": r[2],
                "message": r[3] or None,
                "timestamp": r[4] if len(r) > 4 else None
            }
        ]
    }

@router.get("/stats", response_model=UserStats)
async def get_user_stats(
    current_user: dict = Depends(get_current_user),
    time_period: str = Query("day", description="Periodo di tempo (day, week, month)")
):
    """Ottiene statistiche utente tramite Query Service."""
    uid = current_user["user_id"]
    
    # Determina time range
    end_time = datetime.now()
    if time_period == "day":
        start_time = end_time - timedelta(days=1)
    elif time_period == "week":
        start_time = end_time - timedelta(days=7)
    else:
        start_time = end_time - timedelta(days=30)
        
    # Query al servizio unificato
    result = await query_service_request(
        "/aggregate",
        {
            "metric": "user_stats",
            "dimensions": ["user_id"],
            "filters": {"user_id": uid},
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            }
        }
    )
    
    if result:
        # Estrai dati dalla response
        stats_data = result.get("data", [])
        if stats_data:
            return UserStats(
                total_events=stats_data[0].get("value", 0),
                active_days=stats_data[0].get("count", 0),
                unique_shops=len(stats_data[0].get("dimensions", {}).get("shops", [])),
                notifications=stats_data[0].get("dimensions", {}).get("messages", 0)
            )
    
    # Se Query Service fallisce, usa fallback diretto a ClickHouse
    return await get_user_stats_fallback(current_user, time_period)

async def get_user_stats_fallback(
    current_user: dict,
    time_period: str
) -> UserStats:
    """Fallback per stats se Query Service non disponibile."""
    uid = current_user["user_id"]
    ch_client = CHClient(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse-server"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "nearyou"),
    )
    
    # Determina intervallo di tempo
    now = datetime.now()
    if time_period == "week":
        since = now - timedelta(days=7)
    elif time_period == "month":
        since = now - timedelta(days=30)
    else:  # day è il default
        since = now - timedelta(days=1)
    
    # Query per statistiche
    query = """
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT toDate(event_time)) as active_days,
            COUNT(DISTINCT poi_name) as unique_shops,
            countIf(poi_info != '') as notifications
        FROM user_events
        WHERE user_id = %(uid)s
          AND event_time >= %(since)s
    """
    
    rows = ch_client.execute(query, {
        "uid": uid,
        "since": since.strftime("%Y-%m-%d %H:%M:%S")
    })
    
    if not rows or not rows[0]:
        return UserStats()
    
    return UserStats(
        total_events=rows[0][0],
        active_days=rows[0][1],
        unique_shops=rows[0][2],
        notifications=rows[0][3]
    )

@router.get("/promotions", response_model=PromotionsResponse)
async def get_user_promotions(
    current_user: dict = Depends(get_current_user),
    ch_client: CHClient = Depends(get_clickhouse_client),
    limit: int = Query(10, description="Numero massimo di promozioni da restituire"),
    offset: int = Query(0, description="Offset per la paginazione")
):
    """Ottiene le promozioni ricevute dall'utente."""
    uid = current_user["user_id"]
    
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
    
    rows = ch_client.execute(query, {
        "uid": uid,
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
        
    return {"promotions": result}