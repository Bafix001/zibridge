import requests
import time
from typing import Optional
from loguru import logger
from src.connectors.base import BaseConnector

class RestApiConnector(BaseConnector):
    """
    🛠️ SOCLE TECHNIQUE API ZIBRIDGE.
    Gère la persistance des connexions et la résilience face aux limites d'API.
    """
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        # Session réutilisable pour le multiplexage TCP
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _request(self, method: str, path: str, **kwargs) -> Optional[requests.Response]:
        """
        🚀 Requête résiliente avec gestion automatique des Rate Limits (429)
        et des erreurs temporaires (502, 503, 504).
        """
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        
        # Stratégie de Retry (Elon Mode)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.request(method, url, timeout=30, **kwargs)
                
                # Gestion du Rate Limit
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    logger.warning(f"⏳ Rate Limit (429) sur {url}. Pause de {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                # Gestion des erreurs Gateway temporaires
                if response.status_code in [502, 503, 504] and attempt < max_retries - 1:
                    logger.warning(f"⚠️ Erreur serveur {response.status_code}. Retry {attempt + 1}/{max_retries}...")
                    time.sleep(1 * (attempt + 1))
                    continue

                return response

            except requests.exceptions.RequestException as e:
                logger.error(f"💥 Erreur réseau sur {url}: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)
        
        return None