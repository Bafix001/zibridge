from abc import ABC, abstractmethod
from typing import Generator, Any, Optional, Tuple, Dict, List
from loguru import logger

class BaseConnector(ABC):
    def __init__(
        self,
        credentials: Dict[str, Any],
        project_config: Optional[Dict[str, Any]] = None,
        project_id: Optional[int] = None
    ):
        self.credentials = credentials
        self.project_config = project_config or {}
        self.project_id = project_id
        self.batch_size = 100

        logger.debug(
            f"🧠 BaseConnector initialisé | project_id={self.project_id}"
        )


    @abstractmethod
    def get_available_object_types(self) -> List[str]:
        """
        Découverte dynamique : Retourne la liste de TOUS les types d'objets 
        disponibles dans le CRM (Contacts, Deals, Tickets, CustomObj, etc.)
        """
        pass

    @abstractmethod
    def extract_data(self, object_type: str) -> Generator[Dict[str, Any], None, None]:
        """Extrait n'importe quel type d'objet en streaming."""
        pass
    
    @abstractmethod
    def batch_push_upsert(self, object_type: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Push massif agnostique."""
        pass

    @abstractmethod
    def batch_create_associations(self, associations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format universel : [{'from_id': '..', 'to_id': '..', 'from_type': '..', 'to_type': '..'}]
        """
        pass

    @property
    @abstractmethod
    def source_type(self) -> str:
        pass

    def _extract_id(self, item: Dict[str, Any], object_type: str) -> str:
        """Tente d'extraire l'ID de manière intelligente peu importe l'objet."""
        for key in ["id", f"{object_type}_id", "hs_object_id", "object_id"]:
            if item.get(key): return str(item[key])
        return "UNKNOWN"