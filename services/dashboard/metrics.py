"""Utility per metriche Prometheus."""
import logging
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator, metrics

logger = logging.getLogger(__name__)

def setup_metrics(app: FastAPI, app_name=None):
    """Configura le metriche Prometheus per FastAPI."""
    try:
        service_name = app_name or app.title.lower().replace(" ", "_")
        
        instrumentator = (
            Instrumentator()
            .add(metrics.latency())
            .add(metrics.requests())
            .add(metrics.requests_in_progress())
        )
        
        instrumentator.instrument(app, metric_namespace=service_name)
        instrumentator.expose(app)
        
        logger.info(f"Metriche Prometheus esposte per {service_name} su /metrics")
    except Exception as e:
        logger.error(f"Errore configurazione metriche: {e}")