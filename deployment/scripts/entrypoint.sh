#!/usr/bin/env bash
set -e

# Se non è già impostata, esporta la password per psql
export PGPASSWORD="nearypass"

# 1) Attendi che la tabella 'shops' in Postgres sia disponibile
echo "Attendo la creazione della tabella 'shops' in Postgres..."
until psql -h postgres-postgis -U nearuser -d near_you_shops -c "SELECT 1 FROM shops LIMIT 1;" 2>/dev/null; do
    echo "La tabella 'shops' non è ancora disponibile, riprovo tra 5 secondi..."
    sleep 5
done
echo "Tabella 'shops' trovata. Procedo con la configurazione di Airflow."

# 2) Imposta ownership e permessi su /opt/airflow_home
echo "Imposto ownership e permessi su /opt/airflow_home..."
if chown -R airflow /opt/airflow_home; then
    echo "Ownership impostato correttamente."
else
    echo "Errore nell'impostazione dell'ownership." >&2
    exit 1
fi

if chmod -R 777 /opt/airflow_home; then
    echo "Permessi impostati correttamente."
else
    echo "Errore nell'impostazione dei permessi." >&2
    exit 1
fi

# 3) Inizializzazione e upgrade del DB di Airflow
echo "Inizializzazione e upgrade del database Airflow..."
su airflow -c "airflow db init" || true
su airflow -c "airflow db upgrade"

# 4) Attiva automaticamente il DAG 'etl_shops'
echo "Attivo automaticamente il DAG etl_shops (se presente)..."
su airflow -c "airflow dags unpause etl_shops" || echo "DAG etl_shops già attivo o errore nell'unpause."

# 5) Creazione automatica dell'utenza Admin in Airflow
echo "Creazione automatica dell'utenza Admin in Airflow..."
su airflow -c "airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin" || echo "Utente admin già esistente o errore nella creazione."

# 6) Avvio dello Scheduler come utente 'airflow'
echo "Avvio di Airflow Scheduler come utente 'airflow'..."
exec su airflow -c "airflow scheduler"
