"""Utility per metriche Prometheus."""
import logging
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator, metrics

logger = logging.getLogger(__name__)

def setup_metrics(app: FastAPI, app_name=None):
    """Configura le metriche Prometheus per FastAPI."""
    try:
        service_name = app_name or app.title.lower().replace(" ", "_")
        
        # Crea l'instrumentator con configurazione esplicita
        instrumentator = Instrumentator(
            should_group_status_codes=True,
            should_ignore_untemplated=True,
            should_respect_env_var=True,
            should_instrument_requests_inprogress=True,
            excluded_handlers=[".*admin.*", "/metrics"],
            env_var_name="ENABLE_METRICS",
        )
        
        # Aggiungi le metriche
        instrumentator.add(metrics.latency())
        instrumentator.add(metrics.requests())
        instrumentator.add(metrics.requests_in_progress())
        
        # Configura e esponi le metriche
        instrumentator.instrument(app, metric_namespace=service_name)
        instrumentator.expose(app, endpoint="/metrics", include_in_schema=True)
        
        logger.info(f"Metriche Prometheus esposte per {service_name} su /metrics")
        return True
    except Exception as e:
        logger.error(f"Errore configurazione metriche: {e}")
        # Rilancia l'eccezione per renderla visibile
        raise