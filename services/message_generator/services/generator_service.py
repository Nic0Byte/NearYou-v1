"""
Service per generazione messaggi personalizzati.
"""
import logging
import time
from typing import Dict, Any, Tuple, Optional

from langchain.chat_models import ChatOpenAI
from langchain.schema import HumanMessage
from langchain import PromptTemplate

from ..models.message import UserProfile, PointOfInterest, GeneratedMessage
from .. import cache_utils

logger = logging.getLogger(__name__)

class MessageGeneratorService:
    """
    Service per generare messaggi personalizzati usando LLM.
    Implementa caching e fallback.
    """
    
    def __init__(self, llm_client: ChatOpenAI, prompt_template: str):
        """
        Inizializza il service.
        
        Args:
            llm_client: Client configurato per LLM
            prompt_template: Template per il prompt di generazione
        """
        self.llm_client = llm_client
        self.prompt_template = PromptTemplate(
            input_variables=["age", "profession", "interests", "name", "category", "description"],
            template=prompt_template,
        )
    
    def generate_message(
        self, 
        user_params: Dict[str, Any], 
        poi_params: Dict[str, Any]
    ) -> Tuple[str, bool]:
        """
        Genera un messaggio personalizzato o lo recupera dalla cache.
        
        Args:
            user_params: Parametri dell'utente
            poi_params: Parametri del POI
            
        Returns:
            Tuple[str, bool]: Messaggio e flag se dalla cache
        """
        # Controlla cache
        cached_message = cache_utils.get_cached_message(user_params, poi_params)
        if cached_message:
            logger.info(f"Messaggio trovato in cache per POI {poi_params.get('name', '')}")
            return cached_message, True
        
        # Se non in cache, genera nuovo messaggio
        generated = self._call_llm(user_params, poi_params)
        
        # Salva in cache
        cache_utils.cache_message(user_params, poi_params, generated)
        
        return generated, False
    
    def _call_llm(self, user_params: Dict[str, Any], poi_params: Dict[str, Any]) -> str:
        """
        Effettua chiamata a LLM per generare il messaggio.
        
        Args:
            user_params: Parametri dell'utente
            poi_params: Parametri del POI
            
        Returns:
            str: Messaggio generato
            
        Raises:
            RuntimeError: Se la chiamata al LLM fallisce
        """
        start_time = time.time()
        
        try:
            # Genera il prompt completo
            prompt_text = self.prompt_template.format(
                age=user_params.get("age", 0),
                profession=user_params.get("profession", ""),
                interests=user_params.get("interests", ""),
                name=poi_params.get("name", ""),
                category=poi_params.get("category", ""),
                description=poi_params.get("description", ""),
            )
            
            # Chiamata LLM
            result = self.llm_client([HumanMessage(content=prompt_text)]).content.strip()
            
            # Log tempo di generazione
            generation_time = (time.time() - start_time) * 1000
            logger.info(f"Messaggio generato in {generation_time:.2f}ms per POI {poi_params.get('name', '')}")
            
            return result
        
        except Exception as e:
            logger.error(f"Errore generazione messaggio con LLM: {e}")
            
            # Fallback a messaggio generico
            return self._get_fallback_message(poi_params.get("name", "negozio"), poi_params.get("category", ""))
    
    def _get_fallback_message(self, shop_name: str, category: str) -> str:
        """
        Genera un messaggio di fallback in caso di errore.
        
        Args:
            shop_name: Nome del negozio
            category: Categoria del negozio
            
        Returns:
            str: Messaggio di fallback
        """
        # Semplici template predefiniti per categorie comuni
        templates = {
            "ristorante": f"Sei vicino a {shop_name}! Un ottimo posto per una pausa pranzo gustosa.",
            "bar": f"{shop_name} è a pochi passi! Che ne dici di un ottimo caffè?",
            "abbigliamento": f"Dai un'occhiata alle offerte di {shop_name} proprio qui vicino!",
            "supermercato": f"{shop_name} è qui vicino, perfetto per fare la spesa velocemente.",
        }
        
        # Usa il template della categoria o un generico
        return templates.get(
            category.lower(), 
            f"Sei vicino a {shop_name}! Fermati a dare un'occhiata."
        )