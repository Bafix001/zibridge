from abc import ABC, abstractmethod
from typing import Generator, Any, Optional, Tuple, Dict, List

class BaseConnector(ABC):
    """
    Interface Abstraite Universelle Zibridge.
    Permet l'interopérabilité entre APIs (SaaS) et Fichiers (CSV, Excel).
    """

    @abstractmethod
    def test_connection(self) -> bool:
        """Vérifie l'accès à la source (API Key ou existence du fichier)."""
        pass

    @abstractmethod
    def extract_data(self, object_type: str) -> Generator[Dict[str, Any], None, None]:
        """
        Extrait les données. 
        Chaque dictionnaire retourné DOIT contenir une clé 'id' universelle.
        """
        pass

    @abstractmethod
    def push_update(self, object_type: str, external_id: str, data: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """
        Envoie les modifications. 
        Pour un CSV, cela pourrait signifier réécrire une ligne.
        """
        pass

    @abstractmethod
    def get_association_definition(self, from_type: str, to_type: str) -> Any:
        """Traduit une relation logique en identifiant technique source."""
        pass

    @abstractmethod
    def create_association(self, from_type: str, from_id: str, to_type: str, to_id: str, assoc_type_id: Any) -> bool:
        """Établit le lien entre deux entités."""
        pass

    # --- NOUVELLES MÉTHODES POUR L'AGNOSTICISME ---

    def normalize_data(self, raw_item: Dict[str, Any], object_type: str) -> Dict[str, Any]:
        """
        Optionnel : Transforme les clés propriétaires (ex: 'FirstName') 
        en clés standard Zibridge (ex: 'firstname').
        Par défaut, retourne la donnée brute.
        """
        return raw_item

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Retourne le type de source : 'api', 'file', 'database'."""
        pass