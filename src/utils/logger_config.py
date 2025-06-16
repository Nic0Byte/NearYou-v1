# src/utils/logger_config.py 
import logging
import os
import json
import socket
from datetime import datetime
from typing import Optional

def setup_logging(log_level: Optional[str] = None):
    """
    Configura il logging con formato configurabile e contesto aggiuntivo.
    Mantiene compatibilità con codice esistente.
    
    Args:
        log_level: Livello di log opzionale, altrimenti usa LOG_LEVEL dall'ambiente
    """
    # Configura il livello di log
    level = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Determina il formato (text o json)
    log_format = os.getenv("LOG_FORMAT", "text").lower()
    
    if log_format == "json":
        # Formato JSON per ambienti cloud/prod
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                log_data = {
                    "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                    "path": record.pathname,
                    "line": record.lineno,
                    "function": record.funcName,
                    "service": os.getenv("SERVICE_NAME", "nearyou"),
                    "host": socket.gethostname()
                }
                
                # Aggiungi eccezione se presente
                if record.exc_info:
                    log_data["exception"] = str(record.exc_info[1])
                
                # Aggiungi campi extra
                for k, v in record.__dict__.items():
                    if k not in ["args", "exc_info", "exc_text", "msg", "message", 
                                "levelname", "levelno", "pathname", "filename", 
                                "module", "lineno", "funcName", "created", 
                                "msecs", "relativeCreated", "name", "thread", 
                                "threadName", "processName", "process", "asctime"]:
                        try:
                            json.dumps({k: v})  # Test serializzabilità
                            log_data[k] = v
                        except (TypeError, OverflowError):
                            pass
                
                return json.dumps(log_data)
                
        formatter = JsonFormatter()
    else:
        # Formato leggibile per dev/debug
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )
    
    # Configura handler con nuovo formatter
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    # Imposta configurazione root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Rimuovi handler esistenti per evitare duplicati
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
    
    # Aggiungi il nuovo handler
    root_logger.addHandler(handler)
    
    # Log di informazione inizializzazione
    logging.info(f"Logging inizializzato (livello: {level}, formato: {log_format})")