#!/bin/bash
set -e

echo "--- Inizializzazione viste materializzate per pattern Kappa ---"

echo "Attesa che ClickHouse sia pronto..."
until docker exec -i clickhouse-server clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
    echo "ClickHouse non è ancora pronto, attendo 5 secondi..."
    sleep 5
done

echo "Creazione viste materializzate..."

# Vista aggregata visite orarie per negozio
docker exec -i clickhouse-server clickhouse-client --query "
    CREATE MATERIALIZED VIEW IF NOT EXISTS nearyou.shop_visits_hourly
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(hour)
    ORDER BY (shop_id, hour)
    AS SELECT
        poi_name as shop_id,
        toStartOfHour(event_time) as hour,
        count() as visits,
        uniq(user_id) as unique_visitors,
        avg(poi_range) as avg_distance
    FROM nearyou.user_events
    WHERE poi_name != ''
    GROUP BY shop_id, hour;
"

# Vista attività utente giornaliera
docker exec -i clickhouse-server clickhouse-client --query "
    CREATE MATERIALIZED VIEW IF NOT EXISTS nearyou.user_activity_daily
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(day)
    ORDER BY (user_id, day)
    AS SELECT
        user_id,
        toDate(event_time) as day,
        count() as total_events,
        uniq(poi_name) as unique_shops,
        sum(poi_range) as total_distance
    FROM nearyou.user_events
    GROUP BY user_id, day;
"

# Vista heatmap oraria per zone
docker exec -i clickhouse-server clickhouse-client --query "
    CREATE MATERIALIZED VIEW IF NOT EXISTS nearyou.location_heatmap_hourly
    ENGINE = AggregatingMergeTree()
    ORDER BY (hour, lat_bucket, lon_bucket)
    AS SELECT
        toStartOfHour(event_time) as hour,
        round(latitude, 3) as lat_bucket,
        round(longitude, 3) as lon_bucket,
        count() as activity_count
    FROM nearyou.user_events
    GROUP BY hour, lat_bucket, lon_bucket;
"

echo "Viste materializzate create con successo."