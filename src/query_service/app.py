"""
Query Service - Interfaccia unificata per query su stream e batch.
"""
import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware

from .models import (
    TimeSeriesQuery, AggregateQuery, UserActivityQuery,
    TimeSeriesResponse, AggregateResponse, UserActivityResponse,
    ShopPerformanceQuery, ShopPerformanceResponse
)
from .query_engine import QueryEngine
from .cache_manager import QueryCacheManager

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NearYou Query Service",
    description="Servizio unificato per query su dati real-time e batch",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependencies
query_engine = QueryEngine()
cache_manager = QueryCacheManager()

@app.on_event("startup")
async def startup():
    """Inizializzazioni all'avvio."""
    logger.info("Query Service avviato")
    
@app.on_event("shutdown")
async def shutdown():
    """Cleanup alla chiusura."""
    await query_engine.close()

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "query-service"}

@app.post("/timeseries", response_model=TimeSeriesResponse)
async def query_timeseries(query: TimeSeriesQuery):
    """
    Query time series data con intelligente routing tra stream e batch.
    """
    try:
        # Check cache
        cache_key = cache_manager.get_cache_key("timeseries", query.dict())
        cached = await cache_manager.get(cache_key)
        if cached:
            return TimeSeriesResponse(**cached)
            
        # Decide se usare stream o batch basato su time range
        use_stream = query_engine.should_use_stream(
            query.start_time, 
            query.end_time,
            query.granularity
        )
        
        if use_stream:
            result = await query_engine.query_stream_timeseries(
                metric=query.metric,
                start_time=query.start_time,
                end_time=query.end_time,
                granularity=query.granularity,
                filters=query.filters
            )
        else:
            result = await query_engine.query_batch_timeseries(
                metric=query.metric,
                start_time=query.start_time,
                end_time=query.end_time,
                granularity=query.granularity,
                filters=query.filters
            )
            
        response = TimeSeriesResponse(
            data=result,
            source="stream" if use_stream else "batch",
            cached=False
        )
        
        # Cache result
        await cache_manager.set(cache_key, response.dict(), ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Errore query timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/aggregate", response_model=AggregateResponse)
async def query_aggregate(query: AggregateQuery):
    """
    Query aggregati con routing intelligente.
    """
    try:
        # Per aggregati pesanti usa sempre batch/materialized views
        if query.metric in ["monthly_summary", "shop_performance", "user_journeys"]:
            result = await query_engine.query_batch_aggregate(
                metric=query.metric,
                dimensions=query.dimensions,
                filters=query.filters,
                time_range=query.time_range
            )
        else:
            # Per aggregati semplici può usare stream
            result = await query_engine.query_stream_aggregate(
                metric=query.metric,
                dimensions=query.dimensions,
                filters=query.filters,
                time_range=query.time_range
            )
            
        return AggregateResponse(
            data=result,
            dimensions=query.dimensions
        )
        
    except Exception as e:
        logger.error(f"Errore query aggregate: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/user/activity", response_model=UserActivityResponse)
async def query_user_activity(query: UserActivityQuery):
    """
    Query attività utente combinando stream e batch.
    """
    try:
        # Combina dati real-time e storici
        realtime_data = await query_engine.get_user_realtime_activity(
            user_id=query.user_id,
            hours=24
        )
        
        historical_data = await query_engine.get_user_historical_activity(
            user_id=query.user_id,
            start_date=query.start_date,
            end_date=query.end_date
        )
        
        return UserActivityResponse(
            user_id=query.user_id,
            realtime_activity=realtime_data,
            historical_summary=historical_data
        )
        
    except Exception as e:
        logger.error(f"Errore query user activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/shop/performance", response_model=ShopPerformanceResponse) 
async def query_shop_performance(query: ShopPerformanceQuery):
    """
    Query performance negozio da tabelle pre-aggregate.
    """
    try:
        # Usa sempre dati batch per performance metrics
        metrics = await query_engine.get_shop_performance_metrics(
            shop_ids=query.shop_ids,
            period_days=query.period_days
        )
        
        trends = await query_engine.get_shop_trends(
            shop_ids=query.shop_ids,
            period_days=query.period_days
        )
        
        return ShopPerformanceResponse(
            shops=metrics,
            trends=trends,
            period_days=query.period_days
        )
        
    except Exception as e:
        logger.error(f"Errore query shop performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/sources")
async def get_data_sources():
    """
    Ritorna info su quali data source sono disponibili.
    """
    return {
        "stream": {
            "table": "user_events",
            "retention_days": 7,
            "latency_ms": 100
        },
        "batch": {
            "tables": [
                "monthly_shop_summary",
                "shop_performance_metrics", 
                "user_journey_summary"
            ],
            "update_frequency": "daily",
            "historical_months": 12
        },
        "materialized_views": [
            "shop_visits_hourly",
            "user_activity_daily",
            "location_heatmap_hourly"
        ]
    }

# Configurazione metriche Prometheus
try:
    from src.utils.monitoring.fastapi_metrics import setup_metrics
    setup_metrics(app, app_name="query_service")
except Exception as e:
    logger.warning(f"Metriche non configurate: {e}")