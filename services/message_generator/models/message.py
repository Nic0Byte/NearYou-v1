"""
Modelli dati per messaggi e componenti correlati.
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class UserProfile:
    """Profilo utente per personalizzazione messaggi."""
    age: int
    profession: str
    interests: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserProfile":
        """
        Crea un'istanza da dictionary.
        
        Args:
            data: Dictionary con dati utente
            
        Returns:
            UserProfile: Istanza inizializzata
        """
        return cls(
            age=data.get("age", 0),
            profession=data.get("profession", ""),
            interests=data.get("interests", "")
        )


@dataclass
class PointOfInterest:
    """Punto di interesse (negozio, ristorante, etc.)."""
    name: str
    category: str
    description: Optional[str] = ""
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PointOfInterest":
        """
        Crea un'istanza da dictionary.
        
        Args:
            data: Dictionary con dati POI
            
        Returns:
            PointOfInterest: Istanza inizializzata
        """
        return cls(
            name=data.get("name", ""),
            category=data.get("category", ""),
            description=data.get("description", "")
        )


@dataclass
class GeneratedMessage:
    """Messaggio generato con metadati."""
    content: str
    from_cache: bool = False
    user_profile: Optional[UserProfile] = None
    poi: Optional[PointOfInterest] = None
    generation_time_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converte l'istanza in dictionary.
        
        Returns:
            Dict: Rappresentazione dictionary
        """
        return {
            "content": self.content,
            "from_cache": self.from_cache,
            "generation_time_ms": self.generation_time_ms
        }