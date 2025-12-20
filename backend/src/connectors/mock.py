from src.connectors.base import BaseConnector
from typing import Generator, Any
import time

class MockConnector(BaseConnector):
    """
    Ce connecteur simule une source de données.
    Il respecte le contrat de BaseConnector.
    """

    def test_connection(self) -> bool:
        print("🔍 Simulation : Vérification de la connexion...")
        return True

    def extract_data(self, object_type: str) -> Generator[dict[str, Any], None, None]:
        """
        Simule l'extraction de 3 contacts.
        """
        # Imagine que c'est une base de données géante
        contacts = [
            {"id": "101", "name": "Alice", "email": "alice@zibridge.com"},
            {"id": "102", "name": "Bob", "email": "bob@zibridge.com"},
            {"id": "103", "name": "Charlie", "email": "charlie@zibridge.com"},
        ]

        for contact in contacts:
            # On simule un délai (comme si on appelait l'API HubSpot via internet)
            time.sleep(1) 
            
            # LE MOMENT CLÉ : yield
            # Au lieu de renvoyer toute la liste, on donne UN contact et on 'met en pause'
            print(f"📦 Connecteur : J'ai trouvé {contact['name']}, je l'envoie...")
            yield contact