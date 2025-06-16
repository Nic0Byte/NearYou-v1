#!/usr/bin/env python3
"""
Gestione viste materializzate per pattern Kappa ibrido.
Crea aggregazioni che sarebbero troppo costose da calcolare in real-time.
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any

from clickhouse_driver import Client as CHClient
from src.configg import get_clickhouse_config
from src.utils.logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class MaterializedViewManager:
    """Gestisce creazione e refresh di viste materializzate in ClickHouse."""
    
    def __init__(self):
        self.ch_client = CHClient(**get_clickhouse_config())
        
    def create_aggregate_tables(self):
        """Crea tabelle per aggregati batch (pattern ETL)."""
        tables = [
            # Summary mensile negozi
            """
            CREATE TABLE IF NOT EXISTS nearyou.monthly_shop_summary (
                month Date,
                shop_id String,
                total_visits UInt64,
                unique_visitors UInt64,
                avg_distance Float64,
                calculated_at DateTime
            ) ENGINE = ReplacingMergeTree(calculated_at)
            PARTITION BY toYYYYMM(month)
            ORDER BY (month, shop_id)
            """,
            
            # Performance negozi aggregata
            """
            CREATE TABLE IF NOT EXISTS nearyou.shop_performance_metrics (
                shop_id String,
                period_start DateTime,
                period_end DateTime,
                total_impressions UInt64,
                conversion_rate Float64,
                peak_hour UInt8,
                avg_dwell_time Float64,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (shop_id, period_start)
            """,
            
            # User journey aggregato
            """
            CREATE TABLE IF NOT EXISTS nearyou.user_journey_summary (
                user_id UInt64,
                journey_date Date,
                shops_visited Array(String),
                total_distance Float64,
                journey_duration UInt32,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(journey_date)
            ORDER BY (user_id, journey_date)
            """
        ]
        
        for table_sql in tables:
            try:
                self.ch_client.execute(table_sql)
                logger.info("Tabella aggregata creata/verificata")
            except Exception as e:
                logger.error(f"Errore creazione tabella: {e}")
                
    def refresh_monthly_summary(self):
        """Aggiorna summary mensile negozi (batch job)."""
        try:
            current_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            self.ch_client.execute("""
                INSERT INTO nearyou.monthly_shop_summary
                SELECT
                    %(month)s as month,
                    poi_name as shop_id,
                    count() as total_visits,
                    uniq(user_id) as unique_visitors,
                    avg(poi_range) as avg_distance,
                    now() as calculated_at
                FROM nearyou.user_events
                WHERE event_time >= %(month)s
                  AND event_time < %(month)s + INTERVAL 1 MONTH
                  AND poi_name != ''
                GROUP BY shop_id
            """, {"month": current_month})
            
            logger.info(f"Summary mensile aggiornato per {current_month.strftime('%Y-%m')}")
        except Exception as e:
            logger.error(f"Errore refresh summary mensile: {e}")
            
    def calculate_shop_performance(self, period_days: int = 7):
        """Calcola metriche performance negozi per periodo."""
        try:
            period_start = datetime.now() - timedelta(days=period_days)
            period_end = datetime.now()
            
            self.ch_client.execute("""
                INSERT INTO nearyou.shop_performance_metrics
                SELECT
                    poi_name as shop_id,
                    %(start)s as period_start,
                    %(end)s as period_end,
                    count() as total_impressions,
                    countIf(poi_info != '') / count() as conversion_rate,
                    toHour(argMax(event_time, count())) as peak_hour,
                    avg(poi_range) as avg_dwell_time,
                    now() as updated_at
                FROM nearyou.user_events
                WHERE event_time >= %(start)s
                  AND event_time <= %(end)s
                  AND poi_name != ''
                GROUP BY poi_name
            """, {"start": period_start, "end": period_end})
            
            logger.info(f"Performance metrics calcolate per periodo {period_days} giorni")
        except Exception as e:
            logger.error(f"Errore calcolo performance: {e}")
            
    def aggregate_user_journeys(self):
        """Aggrega percorsi utenti giornalieri."""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).date()
            
            self.ch_client.execute("""
                INSERT INTO nearyou.user_journey_summary
                SELECT
                    user_id,
                    %(date)s as journey_date,
                    groupArray(poi_name) as shops_visited,
                    sum(poi_range) as total_distance,
                    dateDiff('second', min(event_time), max(event_time)) as journey_duration,
                    now() as created_at
                FROM (
                    SELECT *
                    FROM nearyou.user_events
                    WHERE toDate(event_time) = %(date)s
                      AND poi_name != ''
                    ORDER BY user_id, event_time
                )
                GROUP BY user_id
                HAVING length(shops_visited) > 0
            """, {"date": yesterday})
            
            logger.info(f"User journeys aggregati per {yesterday}")
        except Exception as e:
            logger.error(f"Errore aggregazione journeys: {e}")

async def run_batch_aggregations():
    """Entry point per eseguire tutte le aggregazioni batch."""
    manager = MaterializedViewManager()
    
    # Crea tabelle se non esistono
    manager.create_aggregate_tables()
    
    # Esegui aggregazioni
    manager.refresh_monthly_summary()
    manager.calculate_shop_performance()
    manager.aggregate_user_journeys()
    
    logger.info("Aggregazioni batch completate")

if __name__ == "__main__":
    asyncio.run(run_batch_aggregations())