#!/usr/bin/env python3
import os
import ssl
import json
import logging
import argparse
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSourceMessage
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.utils.logger_config import setup_logging
from src.configg import (
    KAFKA_BROKER, KAFKA_TOPIC, CONSUMER_GROUP,
    SSL_CAFILE, SSL_CERTFILE, SSL_KEYFILE,
)
from .operators import (
    DatabaseConnections,
    enrich_with_nearest_shop,
    check_proximity_and_generate_message,
    write_to_clickhouse
)
from src.data_pipeline.stream_replay import StreamReplayManager

logger = logging.getLogger(__name__)
setup_logging()

# Parser per messaggi Kafka
def parse_kafka_message(kafka_msg: KafkaSourceMessage) -> Tuple[str, Dict[str, Any]]:
    """Parsa messaggio Kafka e restituisce (key, value)."""
    try:
        # CORREZIONE: Accedi agli attributi corretti di KafkaSourceMessage
        raw_value = kafka_msg.value  # Invece di trattare msg come valore diretto
        raw_key = kafka_msg.key
        
        # Deserializza il valore
        if isinstance(raw_value, bytes):
            value_str = raw_value.decode("utf-8")
        elif isinstance(raw_value, str):
            value_str = raw_value
        else:
            # Se è già un dict o altro tipo
            value = raw_value
            key = str(value.get("user_id", "unknown")) if isinstance(value, dict) else "unknown"
            return (key, value)
        
        # Parsa JSON
        try:
            value = json.loads(value_str)
        except json.JSONDecodeError as e:
            logger.error(f"Errore parsing JSON: {e}, raw_value: {value_str}")
            return ("error", {"error": f"Invalid JSON: {str(e)}", "raw_value": value_str})
        
        # Usa user_id come chiave per partitioning
        key = str(value.get("user_id", "unknown"))
        
        # Log per debug
        logger.debug(f"Messaggio parsato - Key: {key}, User ID: {value.get('user_id')}")
        
        return (key, value)
        
    except AttributeError as e:
        logger.error(f"Errore accesso attributi messaggio Kafka: {e}")
        logger.error(f"Tipo oggetto ricevuto: {type(kafka_msg)}")
        logger.error(f"Attributi disponibili: {dir(kafka_msg)}")
        return ("error", {"error": f"Attribute error: {str(e)}"})
    except Exception as e:
        logger.error(f"Errore parsing messaggio: {e}")
        logger.error(f"Tipo messaggio: {type(kafka_msg)}")
        return ("error", {"error": str(e)})

# Funzione di validazione dei messaggi
def validate_message(parsed_data: Tuple[str, Dict[str, Any]]) -> bool:
    """Valida che il messaggio parsato sia corretto."""
    key, value = parsed_data
    
    # Filtra messaggi di errore
    if key == "error":
        logger.warning(f"Messaggio con errore filtrato: {value.get('error')}")
        return False
    
    # Verifica presenza campi obbligatori
    if not isinstance(value, dict):
        logger.warning(f"Valore non è un dict: {type(value)}")
        return False
    
    if "user_id" not in value:
        logger.warning("Messaggio senza user_id")
        return False
    
    # Aggiungi altre validazioni secondo le tue necessità
    required_fields = ["user_id"]  # Aggiungi altri campi se necessari
    for field in required_fields:
        if field not in value:
            logger.warning(f"Campo obbligatorio mancante: {field}")
            return False
    
    return True

# Costruzione del dataflow
def build_dataflow() -> Dataflow:
    """Costruisce il dataflow Bytewax."""
    flow = Dataflow("nearyou_consumer")
    
    # Inizializza connessioni database (singleton pattern)
    db_conn = DatabaseConnections()
    
    # 1. Input: leggi da Kafka
    # CORREZIONE: Configurazione separata per consumer (non producer)
    kafka_config = {
        "group.id": CONSUMER_GROUP,
        "security.protocol": "SSL",
        "ssl.ca.location": SSL_CAFILE,
        "ssl.certificate.location": SSL_CERTFILE,
        "ssl.key.location": SSL_KEYFILE,
        "auto.offset.reset": "latest",
        "enable.auto.commit": "false",
        # RIMOSSE: proprietà che causavano warning nei log
        # "enable.auto.commit" e "auto.offset.reset" non vanno nel producer
    }
    
    # Source Kafka
    stream = op.input(
        "kafka_input", 
        flow,
        KafkaSource(
            brokers=[KAFKA_BROKER],
            topics=[KAFKA_TOPIC],
            add_config=kafka_config
        )
    )
    
    # 2. Parse messaggi - CORREZIONE: gestisce KafkaSourceMessage
    parsed = op.map("parse", stream, parse_kafka_message)
    
    # 3. Filtra messaggi errati - CORREZIONE: usa funzione separata
    valid_messages = op.filter("filter_valid", parsed, validate_message)
    
    # 4. Arricchisci con negozio più vicino (async operation)
    enriched = op.flat_map("enrich_shop", valid_messages,
                          lambda x: enrich_with_nearest_shop(x, db_conn))
    
    # 5. Genera messaggio se in prossimità (con cache)
    with_messages = op.flat_map("generate_msg", enriched,
                               lambda x: check_proximity_and_generate_message(x, db_conn))
    
    # 6. Scrivi su ClickHouse (side effect)
    op.inspect("write_clickhouse", with_messages,
               lambda step_id, x: write_to_clickhouse(x, db_conn))
    
    # 7. Log finale per debugging
    op.inspect("log_processed", with_messages,
               lambda step_id, x: logger.info(f"Processato evento per user {x[0]}: "
                                            f"shop={x[1].get('shop_name')} "
                                            f"distance={x[1].get('distance'):.1f}m"))
    
    return flow

# Funzioni per replay mode
def run_replay_mode():
    """Modalità replay per riprocessare eventi storici."""
    parser = argparse.ArgumentParser(description="Replay eventi Kafka")
    parser.add_argument("--hours", type=int, help="Replay ultime N ore")
    parser.add_argument("--start", type=str, help="Start time (ISO format)")
    parser.add_argument("--end", type=str, help="End time (ISO format)")
    parser.add_argument("--user", type=int, help="Filtra per user ID")
    
    args = parser.parse_args()
    
    async def replay_process(event):
        """Processa evento in replay."""
        # Usa stesso processing del flow normale
        db_conn = DatabaseConnections()
        
        # Simula formato Bytewax
        key = str(event.get("user_id", "unknown"))
        value = event
        
        # Processa con operators esistenti
        enriched = enrich_with_nearest_shop((key, value), db_conn)
        if enriched:
            with_msg = check_proximity_and_generate_message(enriched[0], db_conn)
            if with_msg:
                write_to_clickhouse(with_msg[0], db_conn)
                
    async def run_replay():
        manager = StreamReplayManager()
        
        if args.hours:
            await manager.replay_last_n_hours(
                args.hours, 
                replay_process,
                user_filter=[args.user] if args.user else None
            )
        elif args.start and args.end:
            start = datetime.fromisoformat(args.start)
            end = datetime.fromisoformat(args.end)
            await manager.replay_time_range(
                start, end,
                replay_process,
                user_filter=[args.user] if args.user else None
            )
        else:
            print("Specifica --hours o --start/--end per replay")
            
    asyncio.run(run_replay())

# Entry point per Bytewax CLI
if __name__ == "__main__":
    import sys
    
    if "--replay" in sys.argv:
        sys.argv.remove("--replay")
        run_replay_mode()
    else:
        # Normal flow execution
        try:
            # Costruisci il dataflow
            flow = build_dataflow()
            
            # Avvia con CLI (supporta multi-worker, recovery, etc.)
            cli_main(flow)
        except Exception as e:
            logger.error(f"Errore nell'avvio del dataflow: {e}")
            raise