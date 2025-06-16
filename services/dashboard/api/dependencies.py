"""
Dipendenze condivise per le API dashboard - gestisce connessioni DB, auth, ecc.
"""
import os
import logging
from typing import Generator, Dict, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from clickhouse_driver import Client as CHClient

# Constants
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

# ClickHouse setup
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "nearyou")

# OAuth2 setup
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

logger = logging.getLogger(__name__)

def get_clickhouse_client() -> Generator[CHClient, None, None]:
    """
    Dipendenza FastAPI per ottenere un client ClickHouse.
    Yield il client e chiudi la connessione quando terminato.
    """
    client = CHClient(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )
    
    try:
        yield client
    finally:
        
        pass

async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Verifica il token JWT e restituisce il payload dell'utente.
    Solleva HTTPException se il token non è valido.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenziali non valide",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("user_id")
        
        if user_id is None:
            raise credentials_exception
            
        return {"user_id": user_id}
        
    except JWTError as e:
        logger.error(f"Errore JWT: {e}")
        raise credentials_exception