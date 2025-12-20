import json
import hashlib
from typing import Any

def calculate_content_hash(data: dict[str, Any]) -> str:
    """
    Calcule une empreinte unique (SHA-256) pour un dictionnaire.
    """
    # 1. On transforme le dict en chaîne JSON avec les clés triées par ordre alphabétique
    # 'separators' permet d'enlever les espaces inutiles pour avoir toujours le même texte
    canonical_json = json.dumps(data, sort_keys=True, separators=(',', ':'))
    
    # 2. On encode en bytes (le hash travaille sur des octets)
    encoded_data = canonical_json.encode('utf-8')
    
    # 3. On calcule le SHA-256 et on le retourne en format hexadécimal (chaîne de caractères)
    return hashlib.sha256(encoded_data).hexdigest()