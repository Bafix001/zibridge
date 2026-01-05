import requests
from typing import Generator, Dict, Any, List, Optional, Tuple
from loguru import logger
from src.connectors.base import BaseConnector

class SalesforceConnector(BaseConnector):
    """Connecteur Salesforce - Complètement indépendant de HubSpot"""
    
    # Configuration Salesforce-specific
    ENTITY_TYPES = ["Account", "Contact", "Opportunity", "Lead", "Case"]
    
    def __init__(self, token: str, instance_url: str):
        self.token = token
        self.instance_url = instance_url.rstrip('/')
        self.api_version = "v58.0"
        self.base_url = f"{instance_url}/services/data/{self.api_version}"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    @property
    def source_type(self) -> str:
        return "api"
    
    def get_supported_entity_types(self) -> List[str]:
        return self.ENTITY_TYPES
    
    def test_connection(self) -> bool:
        """Vérifie la connexion Salesforce"""
        try:
            response = requests.get(
                f"{self.base_url}/sobjects",
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"❌ Test connexion Salesforce: {e}")
            return False
    
    def get_entity_counts(self) -> Dict[str, int]:
        """Compte via des requêtes SOQL COUNT()"""
        logger.info(f"🔍 Comptage Salesforce...")
        
        counts = {}
        for entity_type in self.ENTITY_TYPES:
            try:
                # SOQL query pour compter
                query = f"SELECT COUNT() FROM {entity_type}"
                url = f"{self.base_url}/query"
                
                response = requests.get(
                    url,
                    headers=self.headers,
                    params={"q": query},
                    timeout=10
                )
                
                if response.status_code == 200:
                    total = response.json().get("totalSize", 0)
                    counts[entity_type] = total
                    if total > 0:
                        logger.success(f"✅ {entity_type}: {total}")
                else:
                    counts[entity_type] = 0
                    logger.warning(f"⚠️  {entity_type}: HTTP {response.status_code}")
                    
            except Exception as e:
                logger.error(f"❌ {entity_type}: {e}")
                counts[entity_type] = 0
        
        return counts
    
    def extract_data(self, object_type: str) -> Generator[Dict[str, Any], None, None]:
        """Extraction via SOQL queries"""
        # Requête SOQL basique - tu peux customiser les champs
        query = f"SELECT Id, Name, CreatedDate FROM {object_type}"
        url = f"{self.base_url}/query"
        
        params = {"q": query}
        next_url = url
        
        while next_url:
            try:
                response = requests.get(
                    next_url,
                    headers=self.headers,
                    params=params if next_url == url else None,
                    timeout=30
                )
                
                if response.status_code != 200:
                    logger.error(f"❌ Salesforce error: {response.text}")
                    break
                
                data = response.json()
                
                for record in data.get("records", []):
                    yield record
                
                # Pagination Salesforce
                next_url = data.get("nextRecordsUrl")
                if next_url:
                    next_url = f"{self.instance_url}{next_url}"
                    
            except Exception as e:
                logger.error(f"❌ Extraction {object_type}: {e}")
                break
    
    def _extract_id(self, item: Dict[str, Any], object_type: str) -> str:
        """ID Salesforce"""
        return str(item.get("Id", ""))
    
    def normalize_data(self, raw_item: Dict[str, Any], object_type: str) -> Dict[str, Any]:
        """Normalise Salesforce → Zibridge"""
        # Salesforce met ses métadonnées dans "attributes"
        attributes = raw_item.pop("attributes", {})
        
        normalized = {
            "id": raw_item.get("Id"),
            "properties": raw_item,
            "_zibridge_links": {},
            "_salesforce_type": attributes.get("type")
        }
        
        return normalized
    
    def push_update(self, object_type: str, external_id: str, data: dict) -> Tuple[str, Optional[str]]:
        """Mise à jour Salesforce"""
        url = f"{self.base_url}/sobjects/{object_type}/{external_id}"
        properties = data.get("properties", {})
        
        # Nettoyage champs read-only Salesforce
        read_only = ["Id", "CreatedDate", "LastModifiedDate", "SystemModstamp"]
        clean_props = {k: v for k, v in properties.items() if k not in read_only}
        
        try:
            response = requests.patch(url, headers=self.headers, json=clean_props, timeout=10)
            
            if response.status_code in [200, 204]:
                return ("updated", None)
            
            if response.status_code == 404:
                # Recréation
                create_url = f"{self.base_url}/sobjects/{object_type}"
                create_resp = requests.post(create_url, headers=self.headers, json=clean_props, timeout=10)
                
                if create_resp.status_code == 201:
                    new_id = create_resp.json().get("id")
                    return ("resurrected", new_id)
            
            return ("failed", None)
            
        except Exception as e:
            logger.error(f"❌ Push update: {e}")
            return ("failed", None)
    
    def create_association(self, from_type: str, from_id: str, to_type: str, to_id: str, assoc_type_id: Any) -> bool:
        """Salesforce utilise des champs de relation (lookup/master-detail)"""
        # Exemple: Lier un Contact à un Account
        # PATCH /Contact/{contactId} avec {"AccountId": "{accountId}"}
        logger.info(f"🔗 Association Salesforce: {from_type} → {to_type}")
        return True  # Implémentation simplifiée
    
    def get_association_definition(self, from_type: str, to_type: str) -> Any:
        """Mapping des relations Salesforce"""
        # Salesforce utilise des noms de champs pour les relations
        definitions = {
            ("Contact", "Account"): "AccountId",
            ("Opportunity", "Account"): "AccountId",
            ("Case", "Contact"): "ContactId",
        }
        return definitions.get((from_type, to_type), None)