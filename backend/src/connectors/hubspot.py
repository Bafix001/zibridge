import requests
import os
from typing import Generator, Any, Optional, Tuple
from loguru import logger
from src.connectors.base import BaseConnector

class HubSpotConnector(BaseConnector):
    def __init__(self, token: str = None):
        self.token = token or os.getenv("HUBSPOT_ACCESS_TOKEN")
        self.base_url = "https://api.hubapi.com/crm/v3/objects"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def test_connection(self) -> bool:
        """Vérifie si le token est valide en appelant les contacts."""
        try:
            response = requests.get(f"{self.base_url}/contacts?limit=1", headers=self.headers)
            return response.status_code == 200
        except Exception:
            return False

    def extract_data(self, object_type: str) -> Generator[dict[str, Any], None, None]:
        """Extrait les données avec les associations v3."""
        # Définition des associations à récupérer pour chaque type
        assoc_map = {
            "contacts": "companies",
            "deals": "companies,contacts",
            "companies": "contacts",
            "tickets": "companies,contacts"
        }
        
        params = f"?limit=100"
        if object_type in assoc_map:
            params += f"&associations={assoc_map[object_type]}"
            
        next_url = f"{self.base_url}/{object_type}{params}"

        while next_url:
            response = requests.get(next_url, headers=self.headers)
            if response.status_code != 200:
                logger.error(f"Erreur HubSpot ({response.status_code}): {response.text}")
                break
            
            data = response.json()
            for item in data.get("results", []):
                yield item

            paging = data.get("paging")
            next_url = paging.get("next", {}).get("link") if paging else None

    def push_update(self, object_type: str, external_id: str, data: dict) -> Tuple[str, Optional[str]]:
        """
        Pousse les modifications vers HubSpot.
        Gère la mise à jour (PATCH) ou la création si l'objet a disparu.
        """
        url = f"{self.base_url}/{object_type}/{external_id}"
        properties = data.get("properties", {})
        
        # Nettoyage des propriétés système en lecture seule
        read_only = ["hs_object_id", "createdate", "lastmodifieddate"]
        payload = {"properties": {k: v for k, v in properties.items() if k not in read_only}}

        # 1. Tentative de mise à jour
        response = requests.patch(url, headers=self.headers, json=payload)
        
        if response.status_code == 200:
            return "updated", None
        
        # 2. Si 404, l'objet a été supprimé -> On le recrée (Resurrection)
        if response.status_code == 404:
            logger.info(f"♻️ Objet {external_id} introuvable, tentative de recréation...")
            create_url = f"{self.base_url}/{object_type}"
            create_resp = requests.post(create_url, headers=self.headers, json=payload)
            
            if create_resp.status_code == 201:
                new_id = create_resp.json().get("id")
                return "resurrected", new_id
                
        return "failed", None

    def create_association(self, from_type: str, from_id: str, to_type: str, to_id: str, assoc_type_id: Any) -> bool:
        """Crée un lien entre deux objets via l'API d'associations HubSpot."""
        url = f"{self.base_url}/{from_type}/{from_id}/associations/{to_type}/{to_id}/{assoc_type_id}"
        try:
            response = requests.put(url, headers=self.headers)
            return response.status_code in [200, 201]
        except Exception as e:
            logger.error(f"Erreur association : {e}")
            return False

    def get_association_definition(self, from_type: str, to_type: str) -> Any:
        """Retourne les IDs d'association standards de HubSpot."""
        definitions = {
            ("contacts", "companies"): 1,  # Contact vers Entreprise
            ("companies", "contacts"): 2,  # Entreprise vers Contact
            ("deals", "contacts"): 3,      # Deal vers Contact
            ("deals", "companies"): 5,     # Deal vers Entreprise
            ("tickets", "contacts"): 16,   # Ticket vers Contact
            ("tickets", "companies"): 26,  # Ticket vers Entreprise
        }
        return definitions.get((from_type, to_type), 1)