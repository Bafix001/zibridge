import json
import os
from typing import List, Dict, Any

class ConfigLoader:
    def __init__(self, path: str = "zibridge_config.json"):
        self.path = path
        self.config = self._load()

    def _load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r") as f:
            return json.load(f)

    @property
    def global_ignore(self) -> List[str]:
        return self.config.get("ignore_columns", [])

    @property
    def batch_size(self) -> int:
        return self.config.get("settings", {}).get("batch_size", 500)

    def get_default_mapping(self, object_type: str) -> Dict[str, Any]:
        return self.config.get("default_mappings", {}).get(object_type, {})

# Instance globale pour tout le projet
ZIBRIDGE_CONFIG = ConfigLoader()
GLOBAL_IGNORE = ZIBRIDGE_CONFIG.global_ignore