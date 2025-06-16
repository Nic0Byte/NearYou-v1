#!/usr/bin/env python3
"""
Supporto replay eventi per pattern Kappa.
Permette di riprocessare eventi storici da Kafka.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
import ssl

from src.configg import (
    KAFKA_BROKER, KAFKA_TOPIC,
    SSL_CAFILE, SSL_CERTFILE, SSL_KEYFILE,
)
from src.utils.logger_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class StreamReplayManager:
    """Gestisce replay di eventi da Kafka per riprocessamento."""
    
    def __init__(self):
        self.consumer = None
        self._setup_ssl()
        
    def _setup_ssl(self):
        """Configura SSL context per Kafka."""
        self.ssl_context = ssl.create_default_context(cafile=SSL_CAFILE)
        self.ssl_context.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)
        
    async def replay_time_range(
        self, 
        start_time: datetime, 
        end_time: datetime,
        process_callback: callable,
        user_filter: Optional[List[int]] = None
    ) -> int:
        """
        Replay eventi in un range temporale.
        
        Args:
            start_time: Timestamp inizio
            end_time: Timestamp fine
            process_callback: Funzione per processare ogni evento
            user_filter: Lista user_id da filtrare (None = tutti)
            
        Returns:
            Numero di eventi processati
        """
        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            security_protocol="SSL",
            ssl_context=self.ssl_context,
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await self.consumer.start()
        
        try:
            # Ottieni partizioni del topic
            partitions = self.consumer.partitions_for_topic(KAFKA_TOPIC)
            if not partitions:
                logger.error("Nessuna partizione trovata per topic")
                return 0
                
            # Cerca offset per timestamp inizio
            start_timestamp_ms = int(start_time.timestamp() * 1000)
            partition_times = {
                TopicPartition(KAFKA_TOPIC, p): start_timestamp_ms 
                for p in partitions
            }
            
            offsets = await self.consumer.offsets_for_times(partition_times)
            
            # Seek agli offset trovati
            for tp, offset_data in offsets.items():
                if offset_data:
                    await self.consumer.seek(tp, offset_data.offset)
                    logger.info(f"Replay da partizione {tp.partition} offset {offset_data.offset}")
                    
            # Processa eventi
            events_processed = 0
            end_timestamp = end_time.timestamp()
            
            async for msg in self.consumer:
                try:
                    # Parse evento
                    event = msg.value
                    event_time = datetime.fromisoformat(event.get("timestamp", ""))
                    
                    # Check se nel range temporale
                    if event_time.timestamp() > end_timestamp:
                        logger.info(f"Raggiunto fine range temporale, stop replay")
                        break
                        
                    # Applica filtro user se specificato
                    if user_filter and event.get("user_id") not in user_filter:
                        continue
                        
                    # Processa evento
                    await process_callback(event)
                    events_processed += 1
                    
                    if events_processed % 1000 == 0:
                        logger.info(f"Processati {events_processed} eventi in replay")
                        
                except Exception as e:
                    logger.error(f"Errore processing evento replay: {e}")
                    continue
                    
            logger.info(f"Replay completato: {events_processed} eventi processati")
            return events_processed
            
        finally:
            await self.consumer.stop()
            
    async def replay_last_n_hours(
        self, 
        hours: int, 
        process_callback: callable,
        user_filter: Optional[List[int]] = None
    ) -> int:
        """Replay eventi delle ultime N ore."""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        return await self.replay_time_range(
            start_time, end_time, process_callback, user_filter
        )
        
    async def rebuild_user_state(self, user_id: int, process_callback: callable) -> Dict[str, Any]:
        """
        Ricostruisce lo stato di un utente riprocessando tutti i suoi eventi.
        
        Args:
            user_id: ID utente
            process_callback: Callback per processare eventi
            
        Returns:
            Stato ricostruito dell'utente
        """
        # Replay ultimi 30 giorni per l'utente specifico
        start_time = datetime.now() - timedelta(days=30)
        end_time = datetime.now()
        
        events_count = await self.replay_time_range(
            start_time, 
            end_time, 
            process_callback,
            user_filter=[user_id]
        )
        
        return {
            "user_id": user_id,
            "events_reprocessed": events_count,
            "rebuild_timestamp": datetime.now().isoformat()
        }

# Esempio callback per riprocessamento
async def example_reprocess_callback(event: Dict[str, Any]):
    """Esempio di callback per riprocessare eventi."""
    # Qui potresti:
    # - Ricalcolare aggregati
    # - Correggere dati
    # - Aggiornare cache
    # - Rigenerare messaggi
    logger.debug(f"Riprocesso evento: user={event.get('user_id')} time={event.get('timestamp')}")