# src/db_utils.py
import time
import logging
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

def wait_for_clickhouse_database(client: Client, db_name: str, timeout: int = 2, max_retries: int = 30) -> bool:
    """
    Attende finché il database specificato esiste in ClickHouse.
    
    Parameters:
        client (Client): Istanza del client ClickHouse.
        db_name (str): Nome del database.
        timeout (int): Tempo in secondi tra i tentativi.
        max_retries (int): Numero massimo di tentativi.
        
    Returns:
        bool: True se il database è disponibile.
    """
    retries = 0
    while retries < max_retries:
        try:
            databases = client.execute("SHOW DATABASES")
            databases_list = [db[0] for db in databases]
            if db_name in databases_list:
                logger.info("Database '%s' trovato.", db_name)
                return True
            else:
                logger.info("Database '%s' non ancora disponibile. Riprovo...", db_name)
        except Exception as e:
            logger.error("Errore nella verifica del database: %s", e)
        time.sleep(timeout)
        retries += 1
    raise Exception(f"Il database '{db_name}' non è stato trovato dopo {max_retries} tentativi.")
