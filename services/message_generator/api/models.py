"""
Modelli Pydantic per request/response nelle API del message generator.
"""
from typing import Optional
from pydantic import BaseModel, Field

class User(BaseModel):
    """Dati utente per personalizzazione messaggio."""
    age: int = Field(..., description="Età dell'utente")
    profession: str = Field(..., description="Professione dell'utente")
    interests: str = Field(..., description="Interessi dell'utente, separati da virgola")

class POI(BaseModel):
    """Point of Interest - negozio o luogo di interesse."""
    name: str = Field(..., description="Nome del POI")
    category: str = Field(..., description="Categoria del POI (es. ristorante, negozio)")
    description: str = Field("", description="Descrizione aggiuntiva del POI")

class GenerateRequest(BaseModel):
    """Request per generazione messaggio personalizzato."""
    user: User
    poi: POI
    
    class Config:
        schema_extra = {
            "example": {
                "user": {
                    "age": 30,
                    "profession": "Ingegnere",
                    "interests": "tecnologia, viaggi, cucina"
                },
                "poi": {
                    "name": "Caffè Milano",
                    "category": "bar",
                    "description": "Negozio a 50m di distanza"
                }
            }
        }

class GenerateResponse(BaseModel):
    """Response con messaggio generato."""
    message: str = Field(..., description="Messaggio personalizzato generato")
    cached: bool = Field(False, description="Indica se il messaggio è stato recuperato dalla cache")
    
    class Config:
        schema_extra = {
            "example": {
                "message": "Ciao! Sei a pochi passi dal Caffè Milano. Il loro cappuccino è perfetto per un ingegnere appassionato di tecnologia come te!",
                "cached": False
            }
        }

class HealthResponse(BaseModel):
    """Risposta per health check."""
    status: str = "ok"
    version: str = "1.0.0"
    provider: str = Field(..., description="Provider LLM in uso")

class CacheStats(BaseModel):
    """Statistiche della cache."""
    enabled: bool
    hits: Optional[int] = None
    misses: Optional[int] = None
    total: Optional[int] = None
    hit_rate: Optional[float] = None
    cache_info: Optional[dict] = None