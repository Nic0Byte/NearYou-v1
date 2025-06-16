"""
Router per le API del message generator.
"""
import os
import logging
from fastapi import APIRouter, Depends, HTTPException, status

from langchain.chat_models import ChatOpenAI
from langchain.schema import HumanMessage
from langchain import PromptTemplate

from .models import GenerateRequest, GenerateResponse, HealthResponse, CacheStats
from .dependencies import get_llm_client, get_prompt_template
from ..services.generator_service import MessageGeneratorService
from .. import cache_utils

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health", response_model=HealthResponse)
async def health():
    """Endpoint per verificare lo stato del servizio."""
    return {
        "status": "ok",
        "version": "1.0.0",
        "provider": os.getenv("LLM_PROVIDER", "openai")
    }

@router.get("/cache/stats", response_model=CacheStats)
async def cache_stats():
    """Endpoint per verificare statistiche cache."""
    return cache_utils.get_cache_stats()

@router.post("/generate", response_model=GenerateResponse)
async def generate(
    req: GenerateRequest,
    llm_client: ChatOpenAI = Depends(get_llm_client)
):
    """
    Genera un messaggio personalizzato in base al profilo utente e POI.
    
    Args:
        req: Richiesta contenente dati utente e POI
        llm_client: Client LLM configurato
        
    Returns:
        GenerateResponse: Messaggio generato e info sulla provenienza (cache o no)
    """
    try:
        # Converti oggetti Pydantic in dict per le funzioni cache
        user_params = req.user.dict()
        poi_params = req.poi.dict()
        
        # Initializza il service
        generator_service = MessageGeneratorService(llm_client, get_prompt_template())
        
        # Genera o recupera il messaggio dalla cache
        message, is_cached = generator_service.generate_message(user_params, poi_params)
        
        return GenerateResponse(message=message, cached=is_cached)
    
    except Exception as e:
        logger.error(f"Errore nella generazione del messaggio: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )