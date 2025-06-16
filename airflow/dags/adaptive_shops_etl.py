# airflow/dags/adaptive_shops_etl.py
"""
DAG Adattivo per ETL Shops - Si auto-configura e decide quando eseguire.
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

import holidays

# Import funzioni dal DAG originale
from etl_shops import extract_data, transform_data, load_data

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'adaptive_shops_etl',
    default_args=default_args,
    description='ETL intelligente che decide quando eseguire',
    schedule_interval='0 5 * * *',  # Check ogni giorno alle 5:00
    catchup=False,
    tags=['etl', 'shops', 'adaptive']
)

def check_if_should_run(**context) -> str:
    """
    Decide se eseguire ETL oggi oppure no.
    Ritorna 'run_etl' o 'skip_etl'.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_postgis')
    
    # 1. Controlla se è il primo run
    try:
        last_run = Variable.get("last_shops_etl_success", default_var=None)
        if not last_run:
            logger.info("PRIMO RUN DETECTED - Eseguo ETL")
            return 'run_etl'
    except:
        logger.info("PRIMO RUN DETECTED - Eseguo ETL")
        return 'run_etl'
    
    # 2. Calcola giorni dall'ultimo run
    last_run_date = datetime.fromisoformat(last_run)
    days_since_last = (datetime.now() - last_run_date).days
    logger.info(f"Giorni dall'ultimo ETL: {days_since_last}")
    
    # 3. Controlla calendario (festività, periodi speciali)
    should_run, reason = check_calendar_triggers()
    if should_run:
        logger.info(f"CALENDAR TRIGGER: {reason}")
        return 'run_etl'
    
    # 4. Controlla schedule base (settimanale)
    if days_since_last >= 7:
        logger.info(f"SCHEDULE TRIGGER: {days_since_last} giorni dall'ultimo run")
        return 'run_etl'
    
    # 5. Controlla anomalie (se abbiamo dati storici)
    try:
        anomaly_detected, anomaly_reason = check_anomalies(pg_hook)
        if anomaly_detected:
            logger.info(f"ANOMALY TRIGGER: {anomaly_reason}")
            return 'run_etl'
    except Exception as e:
        logger.debug(f"Anomaly check fallito (normale al primo run): {e}")
    
    # Default: skip
    logger.info("Nessun trigger attivo - Skip ETL oggi")
    return 'skip_etl'

def check_calendar_triggers() -> Tuple[bool, str]:
    """Controlla se ci sono trigger basati su calendario."""
    today = datetime.now()
    it_holidays = holidays.Italy()
    
    # Festività imminenti
    for i in range(3):  # Controlla prossimi 3 giorni
        check_date = today + timedelta(days=i)
        if check_date.date() in it_holidays:
            return True, f"Festività in arrivo: {it_holidays.get(check_date.date())}"
    
    # Periodi speciali
    month_day = (today.month, today.day)
    
    # Dicembre (shopping natalizio)
    if today.month == 12:
        return True, "Periodo natalizio - frequenza aumentata"
    
    # Gennaio (saldi)
    if today.month == 1 and today.day <= 31:
        return True, "Periodo saldi invernali"
    
    # Luglio (saldi estivi)
    if today.month == 7:
        return True, "Periodo saldi estivi"
    
    # Settembre (back to school/work)
    if today.month == 9 and today.day <= 15:
        return True, "Periodo back to school/work"
    
    return False, ""

def check_anomalies(pg_hook) -> Tuple[bool, str]:
    """Controlla se ci sono anomalie nei pattern di cambiamento."""
    # Query per analizzare cambiamenti recenti
    query = """
    WITH recent_changes AS (
        SELECT 
            DATE(change_time) as day,
            COUNT(*) as changes
        FROM shops_change_log
        WHERE change_time > CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE(change_time)
    ),
    stats AS (
        SELECT 
            AVG(changes) as avg_changes,
            STDDEV(changes) as std_changes,
            MAX(changes) as max_changes
        FROM recent_changes
    )
    SELECT 
        avg_changes,
        std_changes,
        max_changes,
        (SELECT changes FROM recent_changes WHERE day = CURRENT_DATE - INTERVAL '1 day') as yesterday
    FROM stats
    """
    
    result = pg_hook.get_first(query)
    if not result or not result[0]:
        return False, ""
    
    avg, std, max_changes, yesterday = result
    
    # Se ieri ci sono stati molti cambiamenti
    if yesterday and yesterday > avg + (2 * std):
        return True, f"Picco di cambiamenti ieri: {yesterday} vs media {avg:.0f}"
    
    # Se il trend è in crescita
    if max_changes > avg * 3:
        return True, f"Trend in crescita: max {max_changes} vs media {avg:.0f}"
    
    return False, ""

def execute_etl_with_tracking(**context):
    """Esegue ETL tracciando i cambiamenti."""
    start_time = datetime.now()
    
    # 1. Extract (usa funzione esistente)
    raw_data = extract_data()
    logger.info(f"Estratti {len(raw_data)} elementi da OSM")
    
    # 2. Transform (usa funzione esistente)
    shops = transform_data(raw_data)
    logger.info(f"Trasformati {len(shops)} negozi")
    
    # 3. Load con tracking dei cambiamenti
    pg_hook = PostgresHook(postgres_conn_id='postgres_postgis')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Assicurati che la tabella di log esista
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shops_change_log (
            id SERIAL PRIMARY KEY,
            shop_id INTEGER,
            change_type VARCHAR(20),
            change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            old_values JSONB,
            new_values JSONB
        )
    """)
    
    # Contatori
    stats = {"new": 0, "updated": 0, "unchanged": 0}
    
    # Carica negozi tracciando cambiamenti
    for shop in shops:
        # Cerca negozio esistente (per nome e vicinanza)
        cur.execute("""
            SELECT shop_id, shop_name, address, category 
            FROM shops 
            WHERE shop_name = %s 
            AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                100  -- 100 metri di tolleranza
            )
            LIMIT 1
        """, (shop["name"], shop["geom"].split()[0][6:], shop["geom"].split()[1][:-1]))
        
        existing = cur.fetchone()
        
        if not existing:
            # Nuovo negozio
            cur.execute("""
                INSERT INTO shops (shop_name, address, category, geom, created_at)
                VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), CURRENT_TIMESTAMP)
                ON CONFLICT (shop_id) DO NOTHING
                RETURNING shop_id
            """, (shop["name"], shop["address"], shop["category"], shop["geom"]))
            
            new_id = cur.fetchone()
            if new_id:
                stats["new"] += 1
                # Log nuovo negozio
                cur.execute("""
                    INSERT INTO shops_change_log (shop_id, change_type, new_values)
                    VALUES (%s, 'CREATE', %s)
                """, (new_id[0], json.dumps(shop)))
        else:
            # Controlla se ci sono modifiche
            if (existing[1] != shop["name"] or 
                existing[2] != shop["address"] or 
                existing[3] != shop["category"]):
                
                # Aggiorna
                cur.execute("""
                    UPDATE shops 
                    SET shop_name = %s, 
                        address = %s, 
                        category = %s, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE shop_id = %s
                """, (shop["name"], shop["address"], shop["category"], existing[0]))
                
                stats["updated"] += 1
                
                # Log modifiche
                old_values = {
                    "name": existing[1],
                    "address": existing[2],
                    "category": existing[3]
                }
                cur.execute("""
                    INSERT INTO shops_change_log (shop_id, change_type, old_values, new_values)
                    VALUES (%s, 'UPDATE', %s, %s)
                """, (existing[0], json.dumps(old_values), json.dumps(shop)))
            else:
                stats["unchanged"] += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Salva timestamp ultimo run di successo
    Variable.set("last_shops_etl_success", datetime.now().isoformat())
    
    # Log risultati
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"""
    ETL Completato in {duration:.1f} secondi:
    - Nuovi: {stats['new']}
    - Aggiornati: {stats['updated']}
    - Invariati: {stats['unchanged']}
    """)
    
    return stats

# Task definitions
decide_task = BranchPythonOperator(
    task_id='decide_if_run',
    python_callable=check_if_should_run,
    dag=dag
)

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=execute_etl_with_tracking,
    dag=dag
)

skip_task = EmptyOperator(
    task_id='skip_etl',
    dag=dag
)

# Setup tabelle al primo run
setup_tables = PostgresOperator(
    task_id='ensure_tables_exist',
    postgres_conn_id='postgres_postgis',
    sql="""
    -- Aggiungi colonne tracking se non esistono
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    ALTER TABLE shops ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    
    -- Crea tabella log se non esiste
    CREATE TABLE IF NOT EXISTS shops_change_log (
        id SERIAL PRIMARY KEY,
        shop_id INTEGER,
        change_type VARCHAR(20),
        change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        old_values JSONB,
        new_values JSONB
    );
    
    -- Indici per performance
    CREATE INDEX IF NOT EXISTS idx_shops_updated ON shops(updated_at);
    CREATE INDEX IF NOT EXISTS idx_change_log_time ON shops_change_log(change_time);
    """,
    dag=dag
)

# Flow
setup_tables >> decide_task
decide_task >> [run_etl_task, skip_task]