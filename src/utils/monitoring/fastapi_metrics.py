"""
Utility per aggiungere metriche Prometheus ai servizi FastAPI.
Uso:
    from src.utils.monitoring.fastapi_metrics import setup_metrics

    app = FastAPI()
    setup_metrics(app)
"""
import logging
from typing import Optional

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator, metrics

logger = logging.getLogger(__name__)

def setup_metrics(app: FastAPI, app_name: Optional[str] = None) -> None:
    """
    Configura l'instrumentazione Prometheus per l'app FastAPI.
    
    Args:
        app: FastAPI application instance
        app_name: Nome dell'applicazione per etichettare le metriche
    """
    try:
        # Definisci nome dell'applicazione per le metriche
        service_name = app_name or app.title.lower().replace(" ", "_")
        
        # Creazione dell'instrumentator con metriche
        instrumentator = (
            Instrumentator()
            .add(metrics.latency(buckets=(0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)))
            .add(metrics.requests(buckets=(0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)))
            .add(metrics.requests_in_progress())
            .add(metrics.dependency_latency())
            .add(metrics.dependency_requests())
        )
        
        # Aggiunta di labels
        instrumentator.instrument(app, metric_namespace=service_name)
        
        # Esposizione dell'endpoint /metrics
        instrumentator.expose(app)
        
        logger.info(f"Metriche Prometheus esposte per {service_name} su /metrics")
    except Exception as e:
        logger.error(f"Errore configurazione metriche Prometheus: {e}")