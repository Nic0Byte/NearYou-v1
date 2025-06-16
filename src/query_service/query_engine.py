"""
Query Engine - Logica per routing query tra stream e batch.
"""
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional

import asyncpg
from clickhouse_driver import Client as CHClient

from src.configg import (
    get_clickhouse_config, get_postgres_uri,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, 
    POSTGRES_PASSWORD, POSTGRES_DB
)

logger = logging.getLogger(__name__)

class QueryEngine:
    """Engine per eseguire query su diverse sorgenti dati."""
    
    def __init__(self):
        self.ch_client = CHClient(**get_clickhouse_config())
        self._pg_pool = None
        
    async def get_pg_pool(self):
        """Lazy init PostgreSQL pool."""
        if self._pg_pool is None:
            self._pg_pool = await asyncpg.create_pool(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
                min_size=2, max_size=10
            )
        return self._pg_pool
        
    async def close(self):
        """Chiudi connessioni."""
        if self._pg_pool:
            await self._pg_pool.close()
            
    def should_use_stream(
        self, 
        start_time: datetime, 
        end_time: datetime,
        granularity: str
    ) -> bool:
        """
        Decide se usare stream o batch basato su:
        - Recency: dati ultimi 7 giorni -> stream
        - Granularity: minute/hour -> stream, day/month -> batch
        - Range size: < 24h -> stream, > 7 giorni -> batch
        """
        now = datetime.now()
        days_ago = (now - start_time).days
        range_hours = (end_time - start_time).total_seconds() / 3600
        
        # Ultimi 7 giorni e granularità fine
        if days_ago <= 7 and granularity in ["minute", "hour"]:
            return True
            
        # Range piccolo (< 24h)
        if range_hours <= 24:
            return True
            
        # Altrimenti usa batch
        return False
        
    async def query_stream_timeseries(
        self,
        metric: str,
        start_time: datetime,
        end_time: datetime,
        granularity: str,
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Query timeseries da tabella stream (user_events)."""
        # Map metric to SQL
        metric_sql = {
            "visits": "COUNT(*)",
            "unique_users": "uniq(user_id)",
            "avg_distance": "avg(poi_range)",
            "messages": "countIf(poi_info != '')"
        }.get(metric, "COUNT(*)")
        
        # Map granularity
        time_bucket = {
            "minute": "toStartOfMinute(event_time)",
            "hour": "toStartOfHour(event_time)",
            "day": "toDate(event_time)",
            "month": "toStartOfMonth(event_time)"
        }.get(granularity, "toStartOfHour(event_time)")
        
        # Build filters
        where_clauses = [
            "event_time >= %(start_time)s",
            "event_time <= %(end_time)s"
        ]
        params = {
            "start_time": start_time,
            "end_time": end_time
        }
        
        if filters.get("shop_id"):
            where_clauses.append("poi_name = %(shop_id)s")
            params["shop_id"] = filters["shop_id"]
            
        if filters.get("user_id"):
            where_clauses.append("user_id = %(user_id)s")
            params["user_id"] = filters["user_id"]
            
        query = f"""
            SELECT
                {time_bucket} as timestamp,
                {metric_sql} as value
            FROM nearyou.user_events
            WHERE {' AND '.join(where_clauses)}
            GROUP BY timestamp
            ORDER BY timestamp
        """
        
        result = self.ch_client.execute(query, params)
        
        return [
            {
                "timestamp": row[0],
                "value": float(row[1]),
                "metadata": {}
            }
            for row in result
        ]
        
    async def query_batch_timeseries(
        self,
        metric: str,
        start_time: datetime,
        end_time: datetime,
        granularity: str,
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Query timeseries da viste materializzate o tabelle aggregate."""
        # Usa vista materializzata appropriata
        if granularity == "hour":
            table = "shop_visits_hourly"
            time_col = "hour"
            metric_col = {
                "visits": "visits",
                "unique_users": "unique_visitors",
                "avg_distance": "avg_distance"
            }.get(metric, "visits")
        elif granularity == "day":
            table = "user_activity_daily"
            time_col = "day"
            metric_col = {
                "visits": "total_events",
                "unique_shops": "unique_shops",
                "total_distance": "total_distance"
            }.get(metric, "total_events")
        else:
            # Fallback a tabella aggregata mensile
            table = "monthly_shop_summary"
            time_col = "month"
            metric_col = {
                "visits": "total_visits",
                "unique_users": "unique_visitors",
                "avg_distance": "avg_distance"
            }.get(metric, "total_visits")
            
        query = f"""
            SELECT
                {time_col} as timestamp,
                sum({metric_col}) as value
            FROM nearyou.{table}
            WHERE {time_col} >= %(start_time)s
              AND {time_col} <= %(end_time)s
            GROUP BY timestamp
            ORDER BY timestamp
        """
        
        result = self.ch_client.execute(query, {
            "start_time": start_time,
            "end_time": end_time
        })
        
        return [
            {
                "timestamp": row[0],
                "value": float(row[1]),
                "metadata": {"source": "materialized_view"}
            }
            for row in result
        ]
        
    async def query_stream_aggregate(
        self,
        metric: str,
        dimensions: List[str],
        filters: Dict[str, Any],
        time_range: Optional[Any]
    ) -> List[Dict[str, Any]]:
        """Query aggregati da stream."""
        # Implementazione base - personalizza per i tuoi metrics
        metric_sql = {
            "count": "COUNT(*)",
            "unique_users": "uniq(user_id)",
            "avg_distance": "avg(poi_range)"
        }.get(metric, "COUNT(*)")
        
        group_by = ", ".join(dimensions)
        
        query = f"""
            SELECT
                {group_by},
                {metric_sql} as value,
                COUNT(*) as count
            FROM nearyou.user_events
            WHERE event_time >= now() - INTERVAL 24 HOUR
            GROUP BY {group_by}
            ORDER BY value DESC
            LIMIT 100
        """
        
        result = self.ch_client.execute(query)
        
        return [
            {
                "dimensions": dict(zip(dimensions, row[:-2])),
                "value": float(row[-2]),
                "count": row[-1]
            }
            for row in result
        ]
        
    async def query_batch_aggregate(
        self,
        metric: str,
        dimensions: List[str],
        filters: Dict[str, Any],
        time_range: Optional[Any]
    ) -> List[Dict[str, Any]]:
        """Query aggregati da tabelle batch."""
        if metric == "monthly_summary":
            query = """
                SELECT
                    shop_id,
                    month,
                    total_visits as value,
                    unique_visitors as count
                FROM nearyou.monthly_shop_summary
                WHERE month >= now() - INTERVAL 3 MONTH
                ORDER BY total_visits DESC
                LIMIT 50
            """
        elif metric == "shop_performance":
            query = """
                SELECT
                    shop_id,
                    period_start,
                    conversion_rate as value,
                    total_impressions as count
                FROM nearyou.shop_performance_metrics
                WHERE period_end >= now() - INTERVAL 7 DAY
                ORDER BY conversion_rate DESC
            """
        else:
            return []
            
        result = self.ch_client.execute(query)
        
        return [
            {
                "dimensions": {"shop_id": row[0], "period": row[1]},
                "value": float(row[2]),
                "count": row[3]
            }
            for row in result
        ]
        
    async def get_user_realtime_activity(
        self, 
        user_id: int, 
        hours: int = 24
    ) -> Dict[str, Any]:
        """Ottieni attività real-time utente."""
        query = """
            SELECT
                argMax(latitude, event_time) as last_lat,
                argMax(longitude, event_time) as last_lon,
                groupArray(poi_name) as recent_shops,
                count() as events,
                countIf(poi_info != '') as messages
            FROM nearyou.user_events
            WHERE user_id = %(user_id)s
              AND event_time >= now() - INTERVAL %(hours)s HOUR
        """
        
        result = self.ch_client.execute(query, {
            "user_id": user_id,
            "hours": hours
        })
        
        if result:
            row = result[0]
            return {
                "last_position": {"lat": row[0], "lon": row[1]} if row[0] else None,
                "recent_shops": list(set(row[2])) if row[2] else [],
                "active_minutes": hours * 60,  # Semplificato
                "messages_received": row[4]
            }
        
        return {
            "last_position": None,
            "recent_shops": [],
            "active_minutes": 0,
            "messages_received": 0
        }
        
    async def get_user_historical_activity(
        self,
        user_id: int,
        start_date: Optional[date],
        end_date: Optional[date]
    ) -> Dict[str, Any]:
        """Ottieni summary storico da tabelle aggregate."""
        query = """
            SELECT
                count(DISTINCT day) as days_active,
                sum(unique_shops) as total_shops,
                sum(total_distance) / 1000 as total_km
            FROM nearyou.user_activity_daily
            WHERE user_id = %(user_id)s
        """
        
        params = {"user_id": user_id}
        if start_date:
            query += " AND day >= %(start_date)s"
            params["start_date"] = start_date
        if end_date:
            query += " AND day <= %(end_date)s"
            params["end_date"] = end_date
            
        result = self.ch_client.execute(query, params)
        
        # Query negozi preferiti
        shops_query = """
            SELECT
                poi_name,
                count() as visits
            FROM nearyou.user_events
            WHERE user_id = %(user_id)s
              AND poi_name != ''
            GROUP BY poi_name
            ORDER BY visits DESC
            LIMIT 5
        """
        
        shops_result = self.ch_client.execute(shops_query, {"user_id": user_id})
        
        if result:
            row = result[0]
            return {
                "total_days_active": row[0] or 0,
                "total_shops_visited": row[1] or 0,
                "total_distance_km": round(row[2] or 0, 2),
                "favorite_shops": [
                    {"name": shop[0], "visits": shop[1]} 
                    for shop in shops_result
                ],
                "peak_activity_hour": 18  # TODO: calcolare realmente
            }
            
        return {
            "total_days_active": 0,
            "total_shops_visited": 0,
            "total_distance_km": 0,
            "favorite_shops": [],
            "peak_activity_hour": 0
        }
        
    async def get_shop_performance_metrics(
        self,
        shop_ids: List[str],
        period_days: int
    ) -> List[Dict[str, Any]]:
        """Ottieni metriche performance negozi."""
        query = """
            SELECT
                shop_id,
                total_impressions as total_visits,
                unique_visitors,
                conversion_rate,
                peak_hour,
                avg_dwell_time as avg_distance
            FROM nearyou.shop_performance_metrics
            WHERE shop_id IN %(shop_ids)s
              AND period_end >= now() - INTERVAL %(days)s DAY
            ORDER BY period_end DESC
            LIMIT 1 BY shop_id
        """
        
        result = self.ch_client.execute(query, {
            "shop_ids": shop_ids,
            "days": period_days
        })
        
        return [
            {
                "shop_id": row[0],
                "total_visits": row[1],
                "unique_visitors": row[2],
                "conversion_rate": round(row[3], 3),
                "peak_hour": row[4],
                "avg_distance_m": round(row[5], 1)
            }
            for row in result
        ]
        
    async def get_shop_trends(
        self,
        shop_ids: List[str],
        period_days: int
    ) -> List[Dict[str, Any]]:
        """Calcola trend negozi confrontando periodi."""
        # Semplificato - confronta questa settimana vs precedente
        query = """
            WITH current_week AS (
                SELECT
                    poi_name as shop_id,
                    count() as visits
                FROM nearyou.user_events
                WHERE poi_name IN %(shop_ids)s
                  AND event_time >= now() - INTERVAL 7 DAY
                GROUP BY shop_id
            ),
            previous_week AS (
                SELECT
                    poi_name as shop_id,
                    count() as visits
                FROM nearyou.user_events
                WHERE poi_name IN %(shop_ids)s
                  AND event_time >= now() - INTERVAL 14 DAY
                  AND event_time < now() - INTERVAL 7 DAY
                GROUP BY shop_id
            )
            SELECT
                c.shop_id,
                c.visits as current,
                p.visits as previous,
                (c.visits - p.visits) / p.visits as change
            FROM current_week c
            LEFT JOIN previous_week p ON c.shop_id = p.shop_id
        """
        
        result = self.ch_client.execute(query, {"shop_ids": shop_ids})
        
        trends = []
        for row in result:
            change = row[3] if row[3] else 0
            direction = "up" if change > 0.05 else "down" if change < -0.05 else "stable"
            
            trends.append({
                "shop_id": row[0],
                "trend_direction": direction,
                "percent_change": round(change * 100, 1),
                "forecast_next_period": int(row[1] * (1 + change)) if row[1] else None
            })
            
        return trends