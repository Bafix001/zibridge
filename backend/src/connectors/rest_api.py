import requests
import os
from dotenv import load_dotenv
from src.connectors.base import BaseConnector
from typing import Generator, Any, Tuple
from loguru import logger

# Charge le .env pour récupérer le token
load_dotenv()

class RestApiConnector(BaseConnector):
    def __init__(self, token: str = None):
        # Si token fourni, on l'utilise, sinon on prend celui du .env
        self.token = token or os.getenv("HUBSPOT_ACCESS_TOKEN")
        self.base_url = "https://api.hubapi.com/crm/v3/objects"
        
        if not self.token:
            logger.error("❌ HUBSPOT_ACCESS_TOKEN manquant")

    def test_connection(self) -> bool:
        """Vérifie si le token HubSpot est valide."""
        url = f"{self.base_url}/contacts?limit=1"
        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = requests.get(url, headers=headers)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"❌ Test de connexion échoué : {e}")
            return False

    def extract_data(self, object_type: str) -> Generator[dict[str, Any], None, None]:
        """Extrait les données AVEC ASSOCIATIONS et propriétés dynamiques."""
        
        # On définit les associations selon l'objet
        associations_param = ""
        if object_type == "contacts":
            associations_param = "&associations=companies"
        elif object_type == "deals":
            associations_param = "&associations=companies,contacts"
        elif object_type == "companies":
            associations_param = "&associations=contacts"
        elif object_type == "tickets":
            # ✅ Ajout vital pour la Suture des Tickets
            associations_param = "&associations=companies,contacts"
            logger.info("📡 Tickets : associations=companies,contacts activé")
        
        # On ne précise pas 'properties=' pour récupérer le set standard complet de HubSpot
        # ou on adapte selon le type si besoin de champs spécifiques (ex: hs_pipeline)
        next_url = f"https://api.hubapi.com/crm/v3/objects/{object_type}?limit=100{associations_param}"
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        while next_url:
            try:
                response = requests.get(next_url, headers=headers)
                if response.status_code != 200:
                    logger.error(f"❌ Erreur HubSpot ({response.status_code}): {response.text}")
                    break

                data = response.json()
                results = data.get("results", [])
                
                for item in results:
                    yield item

                paging = data.get("paging")
                next_url = paging.get("next", {}).get("link") if paging else None
                
            except Exception as e:
                logger.error(f"💥 Erreur lors de l'extraction de {object_type}: {e}")
                break

    def _extract_existing_id(self, error_response: dict) -> str:
        """Extrait l'ID de l'objet existant depuis le message d'erreur HubSpot."""
        try:
            message = error_response.get("message", "")
            if "Existing ID:" in message:
                existing_id = message.split("Existing ID:")[-1].strip()
                return existing_id
        except:
            pass
        return None

    def push_update(self, object_type: str, item_id: str, data: dict) -> Tuple[str, str]:
        """Restaure un objet (Patch -> Post -> Merge)."""
        url = f"{self.base_url}/{object_type}/{item_id}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        props_to_send = data.get("properties", data)
        forbidden = [
            "hs_object_id", "createdate", "lastmodifieddate", 
            "hs_lastmodifieddate", "hs_createdate", "id",
            "createdAt", "updatedAt"
        ]
        clean_props = {
            k: v for k, v in props_to_send.items() 
            if k not in forbidden and v is not None and v != ""
        }

        try:
            response = requests.patch(url, json={"properties": clean_props}, headers=headers)
            
            if response.status_code in [200, 204]:
                return ("updated", item_id)
            
            if response.status_code == 404:
                logger.warning(f"👻 Objet {item_id} absent. Recréation...")
                create_url = f"{self.base_url}/{object_type}"
                res_create = requests.post(create_url, json={"properties": clean_props}, headers=headers)
                
                if res_create.status_code in [201, 200]:
                    new_id = str(res_create.json().get("id"))
                    return ("resurrected", new_id)
                
                elif res_create.status_code == 409:
                    existing_id = self._extract_existing_id(res_create.json())
                    if existing_id:
                        update_url = f"{self.base_url}/{object_type}/{existing_id}"
                        res_update = requests.patch(update_url, json={"properties": clean_props}, headers=headers)
                        if res_update.status_code in [200, 204]:
                            return ("merged", existing_id)
            
            return ("failed", item_id)
        except Exception as e:
            logger.error(f"💥 Erreur API : {e}")
            return ("failed", item_id)

    def create_association(self, from_type: str, from_id: str, to_type: str, to_id: str, association_type_id: int) -> bool:
        """Crée une association via l'API v4."""
        url = f"https://api.hubapi.com/crm/v4/objects/{from_type}/{from_id}/associations/default/{to_type}/{to_id}"
        # Note : 'default' simplifie la création pour les types standards
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            # Pour une association par défaut, le payload peut être vide ou spécifier le type
            payload = [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": association_type_id}]
            response = requests.put(url, json=payload, headers=headers)
            return response.status_code in [200, 201]
        except Exception as e:
            logger.error(f"💥 Erreur association: {e}")
            return False

    def entity_exists(self, object_type: str, external_id: str) -> bool:
        """Vérifie l'existence d'une entité."""
        url = f"{self.base_url}/{object_type}/{external_id}"
        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            return requests.get(url, headers=headers).status_code == 200
        except:
            return False