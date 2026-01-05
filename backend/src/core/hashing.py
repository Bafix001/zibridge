import json
import hashlib
from typing import Any, Dict, List, Optional
from loguru import logger
from src.utils.config_loader import GLOBAL_IGNORE

def calculate_content_hash(data: Dict[str, Any], project_ignore: Optional[List[str]] = None) -> str:
    """
    💎 Calcule un hash SHA-256 déterministe basé sur les données métier.
    """
    system_blacklist = set(GLOBAL_IGNORE)
    security_blacklist = {
        "_zibridge_meta", 
        "_zibridge_links", 
        "hs_lastmodifieddate",
        "lastmodifieddate",
        "updated_at",
        "id" # On ignore l'ID technique CRM pour le hash (permet de détecter les doublons)
    }
    
    all_ignore = system_blacklist.union(security_blacklist).union(set(project_ignore or []))
    
    # On gère le format HubSpot (dans 'properties') ou le format plat
    payload = data.get("properties", data)
    
    # Nettoyage et normalisation
    business_data = {
        k: str(v) if v is not None else "" 
        for k, v in payload.items() 
        if k not in all_ignore and not k.startswith("_")
    }
    
    canonical_json = json.dumps(
        business_data, 
        sort_keys=True, 
        separators=(',', ':'),
        default=str
    )
    
    return hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()

def calculate_merkle_root(hashes: List[str]) -> str:
    """
    🌳 Calcule le Merkle Root d'une liste de hashs.
    Permet de comparer l'état global d'un CRM en 1 seconde.
    """
    if not hashes:
        return hashlib.sha256(b"empty").hexdigest()
    
    # On trie pour garantir le déterminisme
    sorted_hashes = sorted(hashes)
    combined = "".join(sorted_hashes)
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()