#!/bin/bash
# deployment/scripts/init_postgres.sh
set -xe

echo "--- Inizio script di inizializzazione per PostGIS ---"
echo "Working directory: $(pwd)"
echo "Elenco dei file nella directory:"
ls -l

echo "Attesa iniziale di 60 secondi per il setup di Postgres..."
sleep 60

echo "Attesa che Postgres con PostGIS sia pronto..."

# Imposta la password per psql
export PGPASSWORD=nearypass

COUNTER=0
MAX_RETRIES=40

while true; do
    output=$(psql -h postgres -U nearuser -d near_you_shops -c "SELECT 1" 2>&1) && break
    echo "Tentativo $(($COUNTER+1)): psql non è ancora riuscito. Errore: $output"
    sleep 15
    COUNTER=$(($COUNTER+1))
    if [ $COUNTER -ge $MAX_RETRIES ]; then
         echo "Limite massimo di tentativi raggiunto. Uscita."
         exit 1
    fi
done

echo "Postgres è pronto. Procedo con la creazione delle tabelle..."

psql -h postgres -U nearuser -d near_you_shops <<'EOF'
-- Abilita estensione PostGIS se non già presente
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tabella principale shops
CREATE TABLE IF NOT EXISTS shops (
    shop_id SERIAL PRIMARY KEY,
    shop_name VARCHAR(255),
    address TEXT,
    category VARCHAR(100),
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indici per performance
CREATE INDEX IF NOT EXISTS idx_shops_geom ON shops USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_shops_category ON shops(category);
CREATE INDEX IF NOT EXISTS idx_shops_name ON shops(shop_name);
CREATE INDEX IF NOT EXISTS idx_shops_updated ON shops(updated_at);
CREATE INDEX IF NOT EXISTS idx_shops_created ON shops(created_at);

-- Tabella per tracking cambiamenti (per ETL adattivo)
CREATE TABLE IF NOT EXISTS shops_change_log (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER REFERENCES shops(shop_id),
    change_type VARCHAR(20) NOT NULL, -- CREATE, UPDATE, DELETE
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_values JSONB,
    new_values JSONB
);

-- Indici per change log
CREATE INDEX IF NOT EXISTS idx_change_log_time ON shops_change_log(change_time);
CREATE INDEX IF NOT EXISTS idx_change_log_type ON shops_change_log(change_type);
CREATE INDEX IF NOT EXISTS idx_change_log_shop ON shops_change_log(shop_id);

-- Vista per analisi cambiamenti giornalieri
CREATE OR REPLACE VIEW daily_shop_changes AS
SELECT 
    DATE(change_time) as date,
    COUNT(*) FILTER (WHERE change_type = 'CREATE') as new_shops,
    COUNT(*) FILTER (WHERE change_type = 'UPDATE') as updated_shops,
    COUNT(*) FILTER (WHERE change_type = 'DELETE') as deleted_shops,
    COUNT(*) as total_changes
FROM shops_change_log
GROUP BY DATE(change_time)
ORDER BY date DESC;

-- Trigger per aggiornare updated_at automaticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Rimuovi trigger se esiste (per evitare errori se già presente)
DROP TRIGGER IF EXISTS update_shops_updated_at ON shops;

-- Crea trigger
CREATE TRIGGER update_shops_updated_at 
    BEFORE UPDATE ON shops 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Verifica che tutto sia stato creato
SELECT 
    'Tabelle create:' as info
UNION ALL
SELECT 
    '- ' || tablename 
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename IN ('shops', 'shops_change_log')
UNION ALL
SELECT 
    'Viste create:'
UNION ALL
SELECT 
    '- ' || viewname 
FROM pg_views 
WHERE schemaname = 'public' 
AND viewname = 'daily_shop_changes';

EOF

echo "Inizializzazione di PostGIS completata con successo."