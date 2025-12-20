from abc import ABC, abstractmethod
from typing import Generator, Any

class BaseConnector(ABC):
    """
    C'est une Classe Abstraite. Elle ne peut pas être utilisée seule.
    Elle sert de 'moule' pour tous les futurs connecteurs.
    """

    @abstractmethod
    def test_connection(self) -> bool:
        """Oblige chaque connecteur à avoir une méthode pour vérifier l'accès."""
        pass

    @abstractmethod
    def extract_data(self, object_type: str) -> Generator[dict[str, Any], None, None]:
        """
        Oblige chaque connecteur à extraire des données.
        Le type 'Generator' indique qu'on va utiliser 'yield' (tasse par tasse).
        """
        pass