import requests
import time
from typing import Generator, Any, Optional, Tuple, Dict, List
from loguru import logger
from src.connectors.base import BaseConnector


class HubSpotConnector(BaseConnector):
    """
    🚀 HubSpot Connector "Elon Mode"
    """

    DEFAULT_ASSOC_DEFS = {
        ("contacts", "companies"): 1,
        ("companies", "contacts"): 2,
        ("deals", "companies"): 3,
        ("deals", "contacts"): 4,
        ("tickets", "companies"): 25,
        ("tickets", "contacts"): 15,
    }

    def __init__(
        self,
        token: str,
        credentials: Dict = None,
        project_config: Dict = None,
        project_id: Optional[int] = None
    ):
        super().__init__(
            credentials=credentials or {},
            project_config=project_config or {},
            project_id=project_id
        )
        self.token = token

        logger.debug(
            f"🔌 HubSpotConnector prêt | project_id={self.project_id}"
        )

        self.base_url = "https://api.hubapi.com/crm/v3"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    @property
    def source_type(self) -> str:
        return "hubspot"

    # ===================== HTTP =====================

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """HTTP helper avec retry + support URLs absolues"""
        if path.startswith("http"):
            url = path
        else:
            url = f"{self.base_url}/{path.lstrip('/')}"

        for _ in range(3):
            response = requests.request(method, url, headers=self.headers, **kwargs)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2))
                logger.warning(f"⏳ Rate limit HubSpot — pause {retry_after}s")
                time.sleep(retry_after)
                continue
            return response
        return response

    # ===================== HEALTH =====================

    def test_connection(self) -> bool:
        res = self._request("GET", "objects/contacts?limit=1")
        return res.status_code == 200

    # ===================== SCHEMAS =====================

    def get_available_object_types(self) -> List[str]:
        """Découverte dynamique avec fallback"""
        try:
            res = self._request("GET", "schemas")
            if res.status_code == 200:
                schemas = res.json().get("results", [])
                objects = [s.get("name") for s in schemas if s.get("name")]
                if objects:
                    return objects
        except Exception as e:
            logger.warning(f"⚠️ Schemas HubSpot indisponibles: {e}")

        return ["contacts", "companies", "deals"]

    # ===================== EXTRACTION =====================

    def extract_entities(self, object_type: str) -> Generator[Dict[str, Any], None, None]:
        mapping = self.project_config.get("mappings", {}).get(object_type, {})
        fields = mapping.get(
            "properties",
            ["email", "firstname", "lastname", "name", "domain"]
        )

        fields_str = ",".join(fields) if isinstance(fields, list) else fields

        assoc_cfg = self.project_config.get("associations", {}).get(
            object_type, ["companies", "contacts"]
        )
        assoc_str = ",".join(assoc_cfg)

        params = {
            "limit": 100,
            "properties": fields_str,
            "associations": assoc_str
        }

        path = f"objects/{object_type}"

        while path:
            res = self._request("GET", path, params=params if not path.startswith("http") else None)

            if res.status_code != 200:
                logger.error(f"❌ HubSpot {object_type} HTTP {res.status_code}")
                break

            data = res.json()
            results = data.get("results", [])

            for item in results:
                item["_zibridge_links"] = self._parse_hubspot_associations(item)
                yield item

            path = data.get("paging", {}).get("next", {}).get("link")

    def extract_data(self, object_type: str) -> Generator[Dict[str, Any], None, None]:
        return self.extract_entities(object_type)

    # ===================== ASSOCIATIONS =====================

    def _parse_hubspot_associations(self, item: Dict) -> Dict[str, List[str]]:
        links = {}
        assocs = item.get("associations", {})

        for target_type, content in assocs.items():
            ids = [
                str(r["id"])
                for r in content.get("results", [])
                if r.get("id")
            ]
            if ids:
                links[target_type] = ids

        return links

    def get_associations(self, object_type: str, object_id: str) -> Dict[str, List[str]]:
        """
        🔗 Récupère toutes les associations d'un objet HubSpot via API v4.
        """
        associations = {}
        
        association_types = {
            "contacts": ["companies", "deals"],
            "companies": ["contacts", "deals"],
            "deals": ["contacts", "companies"]
        }
        
        if object_type not in association_types:
            return associations
        
        for assoc_type in association_types[object_type]:
            try:
                url = f"https://api.hubapi.com/crm/v4/objects/{object_type}/{object_id}/associations/{assoc_type}"
                response = self._request("GET", url)
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get("results", [])
                    
                    if results:
                        assoc_ids = [str(result["toObjectId"]) for result in results]
                        associations[assoc_type] = assoc_ids
                        logger.debug(f"✅ {len(assoc_ids)} associations {assoc_type} pour {object_type}:{object_id}")
                
            except Exception as e:
                logger.debug(f"Pas d'association {assoc_type} pour {object_type} #{object_id}: {e}")
        
        return associations

    def get_association_definition(self, source_type: str, target_type: str) -> int:
        return self.DEFAULT_ASSOC_DEFS.get((source_type, target_type), 1)

    # Dans src/connectors/hubspot.py

    def create_association(
    self, 
    from_type: str, 
    from_id: str, 
    to_type: str, 
    to_id: str
) -> bool:
        """🔗 Crée une association entre deux objets HubSpot via API v4."""
        try:
            assoc_id = self.get_association_definition(from_type, to_type)
            url = f"https://api.hubapi.com/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}/{to_id}"
            
            payload = [{
                "associationCategory": "HUBSPOT_DEFINED",
                "associationTypeId": assoc_id
            }]
            
            response = self._request("PUT", url, json=payload)
            
            if response.status_code in (200, 201, 204):
                logger.success(f"✅ Association créée : {from_type}:{from_id} → {to_type}:{to_id}")
                return True
            else:
                logger.error(f"❌ Erreur association ({response.status_code}): {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Exception création association : {e}")
            return False

    def batch_create_associations(self, associations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not associations:
            return []

        from_type = associations[0]["from_type"]
        to_type = associations[0]["to_type"]
        assoc_id = self.get_association_definition(from_type, to_type)

        path = f"associations/{from_type}/{to_type}/batch/create"

        inputs = [
            {
                "from": {"id": a["from_id"]},
                "to": {"id": a["to_id"]},
                "types": [{
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": assoc_id
                }]
            }
            for a in associations
        ]

        res = self._request("POST", path, json={"inputs": inputs})
        return res.json().get("results", []) if res.status_code in (200, 201, 207) else []

    # ===================== UPSERT / CREATE / UPDATE =====================

    def push_create(self, object_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """✅ Crée un nouvel objet."""
        path = f"objects/{object_type}"
        payload = {"properties": self._clean_props(properties)}
        
        response = self._request("POST", path, json=payload)
        
        if response.status_code in (200, 201):
            result = response.json()
            logger.success(f"✅ Créé : {object_type} #{result.get('id')}")
            return result
        else:
            logger.error(f"❌ Erreur création {object_type}: {response.status_code}")
            raise Exception(f"Failed to create {object_type}: {response.text}")

    def batch_push_upsert(self, object_type: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        path = f"objects/{object_type}/batch/create"
        payload = {"inputs": [{"properties": self._clean_props(i)} for i in items]}

        res = self._request("POST", path, json=payload)
        if res.status_code in (200, 201):
            results = res.json().get("results", [])
            logger.success(f"⚡ {len(results)} {object_type} synchronisés")
            return results

        return []

    def push_update(self, object_type: str, external_id: str, data: Dict) -> Tuple[str, Optional[str]]:
        path = f"objects/{object_type}/{external_id}"
        payload = {"properties": self._clean_props(data)}
        res = self._request("PATCH", path, json=payload)
        return ("updated", None) if res.status_code < 400 else ("error", res.text)

    def batch_update(self, object_type: str, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """📝 Met à jour plusieurs objets en batch."""
        if not items:
            return []
        
        path = f"objects/{object_type}/batch/update"
        
        inputs = [
            {
                "id": item["id"],
                "properties": self._clean_props(item["properties"])
            }
            for item in items
        ]
        
        payload = {"inputs": inputs}
        response = self._request("POST", path, json=payload)
        
        if response.status_code in (200, 201):
            results = response.json().get("results", [])
            logger.success(f"📝 {len(results)} {object_type} mis à jour")
            return results
        else:
            logger.error(f"❌ Erreur batch update : {response.status_code}")
            return []

    def delete(self, object_type: str, object_id: str) -> bool:
        """🗑️ Supprime un objet."""
        path = f"objects/{object_type}/{object_id}"
        response = self._request("DELETE", path)
        
        if response.status_code == 204:
            logger.success(f"🗑️ Supprimé : {object_type} #{object_id}")
            return True
        else:
            logger.error(f"❌ Erreur suppression : {response.status_code}")
            return False

    # ===================== UTILS =====================

    def _clean_props(self, data: Dict) -> Dict:
        forbidden = {"id", "createdate", "updatedate", "hs_object_id", "archived"}
        return {
            k: v for k, v in data.items()
            if k not in forbidden and not k.startswith("_")
        }

    def normalize_data(self, data: Dict[str, Any], object_type: str = None) -> Dict[str, Any]:
        props = data.get("properties")
        if isinstance(props, dict):
            return props
        return {k: v for k, v in data.items() if not k.startswith("_")}