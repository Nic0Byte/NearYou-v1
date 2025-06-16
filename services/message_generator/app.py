# services/message_generator/app.py

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Importa il router dalle api
from .api import api_router

# Configurazione applicazione
app = FastAPI(
    title="NearYou Message Generator",
    description="Servizio di generazione messaggi personalizzati per l'app NearYou",
    version="1.0.0"
)

# Configurazione CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Aggiungi router principale
app.include_router(api_router)

# Configurazione middleware e logging avanzato
if os.getenv("ENVIRONMENT") == "production":
    # Solo in produzione aggiungiamo questi middleware
    import time
    from fastapi import Request
    
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        """Aggiunge header con tempo di elaborazione."""
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response

# /health sia sempre attivo
@app.get("/", include_in_schema=False)
async def root():
    """Reindirizza alla documentazione dell'API."""
    return {"message": "NearYou Message Generator API", "docs": "/docs"}

# Configurazione metriche Prometheus
try:
    from .metrics import setup_metrics
    setup_metrics(app, app_name="message_generator")
    print("Metriche Prometheus configurate con successo")
except Exception as e:
    print(f"Errore configurazione metriche: {e}")