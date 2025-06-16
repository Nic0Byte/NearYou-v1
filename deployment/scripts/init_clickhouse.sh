#!/bin/bash
set -e

echo "--- Inizio script di inizializzazione ---"
echo "Working directory: $(pwd)"
echo "Elenco dei file nella directory:"
ls -l

echo "Attesa che ClickHouse sia pronto..."

until docker exec -i clickhouse-server clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do
    echo "ClickHouse non è ancora pronto, attendo 5 secondi..."
    sleep 5
done

echo "ClickHouse è pronto. Procedo con la creazione."

# Creazione del database se non esiste
echo "Creazione del database 'nearyou' (se non esiste già)..."
docker exec -i clickhouse-server clickhouse-client --query "CREATE DATABASE IF NOT EXISTS nearyou;"

# Creazione della tabella users all'interno del database 'nearyou'
echo "Creazione della tabella users..."
docker exec -i clickhouse-server clickhouse-client --query "
    USE nearyou;
    CREATE TABLE IF NOT EXISTS users (
        user_id           UInt64,
        username          String,
        full_name         String,
        email             String,
        phone_number      String,
        password          String,
        user_type         String,
        gender            String,
        age               UInt32,
        profession        String,
        interests         String,
        country           String,
        city              String,
        registration_time DateTime
    ) ENGINE = MergeTree()
    ORDER BY user_id;
"

# Creazione della tabella user_events all'interno del database 'nearyou'
echo "Creazione della tabella user_events..."
docker exec -i clickhouse-server clickhouse-client --query "
    USE nearyou;
    CREATE TABLE IF NOT EXISTS user_events (
        event_id   UInt64,
        event_time DateTime,
        user_id    UInt64,
        latitude   Float64,
        longitude  Float64,
        poi_range  Float64,
        poi_name   String,
        poi_info   String
    ) ENGINE = MergeTree()
    ORDER BY event_id;
"

# Creazione tabelle aggregate per pattern Kappa ibrido
echo "Creazione tabelle aggregate..."

# Monthly shop summary
docker exec -i clickhouse-server clickhouse-client --query "
    USE nearyou;
    CREATE TABLE IF NOT EXISTS monthly_shop_summary (
        month Date,
        shop_id String,
        total_visits UInt64,
        unique_visitors UInt64,
        avg_distance Float64,
        calculated_at DateTime
    ) ENGINE = ReplacingMergeTree(calculated_at)
    PARTITION BY toYYYYMM(month)
    ORDER BY (month, shop_id);
"

# Shop performance metrics
docker exec -i clickhouse-server clickhouse-client --query "
    USE nearyou;
    CREATE TABLE IF NOT EXISTS shop_performance_metrics (
        shop_id String,
        period_start DateTime,
        period_end DateTime,
        total_impressions UInt64,
        conversion_rate Float64,
        peak_hour UInt8,
        avg_dwell_time Float64,
        updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (shop_id, period_start);
"

# User journey summary
docker exec -i clickhouse-server clickhouse-client --query "
    USE nearyou;
    CREATE TABLE IF NOT EXISTS user_journey_summary (
        user_id UInt64,
        journey_date Date,
        shops_visited Array(String),
        total_distance Float64,
        journey_duration UInt32,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(journey_date)
    ORDER BY (user_id, journey_date);
"

echo "Tabelle aggregate create."

# Inizializza viste materializzate
echo "Inizializzazione viste materializzate..."
if [ -f /workspace/deployment/scripts/init_materialized_views.sh ]; then
    bash /workspace/deployment/scripts/init_materialized_views.sh
fi

echo "Inizializzazione di ClickHouse completata."