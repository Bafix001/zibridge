import json
import hashlib
from typing import Any, Dict, List, Optional
from loguru import logger
from src.utils.config_loader import GLOBAL_IGNORE

def calculate_content_hash(
    data: Dict[str, Any], 
    project_ignore: Optional[List[str]] = None, 
    include_links: bool = True
) -> str:
    """
    💎 Calcule un hash SHA-256 déterministe basé sur les données métier.
    Si include_links=True, inclut aussi les _zibridge_links dans le hash.
    """
    system_blacklist = set(GLOBAL_IGNORE)
    security_blacklist = {
        "_zibridge_meta",
        "hs_lastmodifieddate",
        "lastmodifieddate",
        "updated_at",
        "id"
    }
    all_ignore = system_blacklist.union(security_blacklist).union(set(project_ignore or []))
    
    payload = data.get("properties", data)
    business_data = {
        k: str(v) if v is not None else ""
        for k, v in payload.items()
        if k not in all_ignore and not k.startswith("_")
    }
    
    if include_links:
        # Normalisation des links pour hash stable
        links = data.get("_zibridge_links", {})
        normalized_links = normalize_links_for_hash(links)
        business_data["_zibridge_links"] = normalized_links
    
    canonical_json = json.dumps(business_data, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()

def normalize_links_for_hash(links: Any) -> Dict[str, List[str]]:
    """
    🔗 Normalise les liens vers un format dict stable pour le hashing.
    Supporte: dict, list[dict], list[str]
    """
    if not links:
        return {}
    
    normalized = {}
    
    # Format dict (votre format actuel)
    if isinstance(links, dict):
        for rel_type, ids in links.items():
            id_list = ids if isinstance(ids, list) else [ids]
            normalized[rel_type] = sorted([str(i) for i in id_list])
    
    # Format list[dict]
    elif isinstance(links, list):
        for item in links:
            if isinstance(item, dict) and "id" in item:
                rel_type = item.get("type", "unknown")
                if rel_type not in normalized:
                    normalized[rel_type] = []
                normalized[rel_type].append(str(item["id"]))
            elif isinstance(item, str):
                # Format "type:id"
                if ":" in item:
                    rel_type, rel_id = item.split(":", 1)
                    if rel_type not in normalized:
                        normalized[rel_type] = []
                    normalized[rel_type].append(rel_id)
    
    # Tri pour stabilité
    for k in normalized:
        normalized[k] = sorted(normalized[k])
    
    return normalized

def calculate_merkle_root(hashes: List[str]) -> str:
    """
    🌳 Calcule le Merkle Root d'une liste de hash.
    """
    if not hashes:
        return hashlib.sha256(b"empty").hexdigest()
    sorted_hashes = sorted(hashes)
    combined = "".join(sorted_hashes)
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()