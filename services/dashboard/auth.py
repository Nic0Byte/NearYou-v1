# services/dashboard/auth.py
import os
import time
import logging
from typing import Dict, Any, Optional

from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

# Configurazione JWT
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRATION_S = int(os.getenv("JWT_EXPIRATION_S", "3600"))

# Configurazione ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "nearyou")

# Setup OAuth2
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

# Client ClickHouse per login
ch = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
)

def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Autentica un utente verificando username e password.
    
    Args:
        username: Nome utente
        password: Password
        
    Returns:
        Dict con informazioni utente se autenticato, None altrimenti
    """
    try:
        q = "SELECT user_id, password FROM users WHERE username = %(u)s LIMIT 1"
        rows = ch.execute(q, {"u": username})
        
        if not rows:
            logger.warning(f"Tentativo login fallito: utente {username} non trovato")
            return None
            
        user_id, pw = rows[0]
        
        # In un sistema reale, qui useremmo passlib per verifica password sicura
        # Es: if not pwd_context.verify(password, pw):
        if pw != password:
            logger.warning(f"Tentativo login fallito: password errata per {username}")
            return None
            
        logger.info(f"Login utente {username} (ID: {user_id}) completato con successo")
        return {"user_id": user_id}
        
    except Exception as e:
        logger.error(f"Errore durante autenticazione utente {username}: {e}")
        return None

def create_access_token(data: Dict[str, Any]) -> str:
    """
    Crea un token JWT con i dati forniti e scadenza.
    
    Args:
        data: Dati da includere nel token
        
    Returns:
        Token JWT
    """
    to_encode = data.copy()
    
    # Aggiungi scadenza 
    expiration = time.time() + JWT_EXPIRATION_S
    to_encode["exp"] = expiration
    
    # Aggiungi timestamp di creazione
    to_encode["iat"] = time.time()
    
    try:
        # Genera il token
        token = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
        return token
    except Exception as e:
        logger.error(f"Errore generazione token JWT: {e}")
        raise

def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Verifica il token JWT e restituisce il payload dell'utente.
    Solleva HTTPException se il token non è valido.
    
    Args:
        token: Token JWT da verificare
        
    Returns:
        Dictionary con dati utente
        
    Raises:
        HTTPException: Se le credenziali non sono valide
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenziali non valide",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Decodifica il token
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        
        # Estrai user_id
        user_id: int = payload.get("user_id")
        
        if user_id is None:
            raise credentials_exception
            
        return {"user_id": user_id}
        
    except JWTError as e:
        logger.error(f"Errore verifica JWT: {e}")
        raise credentials_exception
    except Exception as e:
        logger.error(f"Errore generico in get_current_user: {e}")
        raise credentials_exception

# Funzione per il refresh token (estensione futura)
def refresh_token(token: str) -> str:
    """
    Crea un nuovo token basato su un token esistente.
    Utile per implementare refresh token.
    
    Args:
        token: Token JWT esistente
        
    Returns:
        Nuovo token JWT
        
    Raises:
        HTTPException: Se il token non è valido
    """
    try:
        # Decodifica il token esistente senza verificare la scadenza
        payload = jwt.decode(
            token, 
            JWT_SECRET, 
            algorithms=[JWT_ALGORITHM],
            options={"verify_exp": False}
        )
        
        # Crea un nuovo token con lo stesso user_id
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token non valido per refresh"
            )
            
        return create_access_token({"user_id": user_id})
        
    except JWTError as e:
        logger.error(f"Errore refresh token: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Impossibile aggiornare il token"
        )