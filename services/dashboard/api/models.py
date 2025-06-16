"""
Modelli Pydantic per request/response nelle API dashboard.
"""
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

# ---- Auth Models ----

class Token(BaseModel):
    """Token di accesso JWT."""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """Dati contenuti nel payload JWT."""
    user_id: int


class LoginRequest(BaseModel):
    """Dati richiesta login."""
    username: str
    password: str


# ---- User Models ----

class UserProfile(BaseModel):
    """Profilo utente."""
    user_id: int
    age: int
    profession: str
    interests: str


class Position(BaseModel):
    """Posizione GPS utente."""
    user_id: int
    latitude: float
    longitude: float
    message: Optional[str] = None
    timestamp: Optional[datetime] = None


class PositionsResponse(BaseModel):
    """Risposta con elenco posizioni utente."""
    positions: List[Position]


class Shop(BaseModel):
    """Negozio con posizione."""
    id: int
    shop_name: str
    category: str
    lat: float
    lon: float
    distance: Optional[float] = None


class Promotion(BaseModel):
    """Promozione ricevuta dall'utente."""
    event_id: int
    timestamp: datetime
    shop_name: str
    message: str


class PromotionsResponse(BaseModel):
    """Risposta con elenco promozioni utente."""
    promotions: List[Promotion]


class UserStats(BaseModel):
    """Statistiche utente."""
    total_events: int = 0
    active_days: int = 0
    unique_shops: int = 0
    notifications: int = 0