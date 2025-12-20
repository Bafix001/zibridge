import json
import hashlib
from pathlib import Path

# Chargement de la config
config_path = Path("zibridge_config.json")
IGNORE_COLUMNS = []
if config_path.exists():
    with open(config_path) as f:
        IGNORE_COLUMNS = json.load(f).get("ignore_columns", [])

def calculate_content_hash(data: dict) -> str:
    # On crée une copie pour ne pas modifier la donnée originale
    clean_data = {k: v for k, v in data.items() if k not in IGNORE_COLUMNS}
    
    # On trie les clés pour que le hash soit déterministe
    content_str = json.dumps(clean_data, sort_keys=True)
    return hashlib.sha256(content_str.encode()).hexdigest()

