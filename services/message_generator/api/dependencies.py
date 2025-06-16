"""
Dipendenze condivise per il servizio message_generator.
"""
import os
import logging
from typing import Callable, Any, Dict

from langchain.chat_models import ChatOpenAI

logger = logging.getLogger(__name__)

# Configurazione LLM
PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()
BASE_URL = os.getenv("OPENAI_API_BASE") or None
API_KEY = os.getenv("OPENAI_API_KEY")

def get_llm_client() -> ChatOpenAI:
    """
    Fornisce un client LLM configurato.
    
    Returns:
        ChatOpenAI: Client configurato per interagire con il modello LLM
    
    Raises:
        RuntimeError: Se la configurazione è mancante
    """
    if PROVIDER in {"openai", "groq", "together", "fireworks"} and not API_KEY:
        raise RuntimeError("OPENAI_API_KEY mancante per il provider scelto")
    
    # Seleziona il modello in base al provider
    if PROVIDER == "openai":
        model_name = "gpt-4o-mini"
    elif PROVIDER == "groq":
        model_name = "gemma2-9b-it"
    else:
        model_name = "gpt-3.5-turbo"  # Default fallback
    
    logger.info(f"Inizializzazione LLM client con provider: {PROVIDER}, modello: {model_name}")
    
    return ChatOpenAI(
        model=model_name,
        temperature=0.7,
        openai_api_key=API_KEY,
        openai_api_base=BASE_URL,
    )

def get_prompt_template():
    """
    Restituisce il template per il prompt di generazione messaggi.
    
    Returns:
        str: Template del prompt formattabile
    """
    return """Sei un sistema di advertising che crea un messaggio conciso e coinvolgente.
Utente:
- Età: {age}
- Professione: {profession}
- Interessi: {interests}

Negozio:
- Nome: {name}
- Categoria: {category}
- Descrizione aggiuntiva: {description}

Condizioni:
- L'utente è a pochi metri dal negozio.
- Il messaggio deve essere breve (max 30 parole) e invogliare l'utente a fermarsi.

Genera il messaggio in italiano:"""