import os
from typing import Dict, Any, Optional
from src.connectors.hubspot import HubSpotConnector
from src.connectors.file_connector import FileConnector
# from src.connectors.salesforce import SalesforceConnector  # Futur
from loguru import logger

class ConnectorFactory:
    """
    🏗️ LE CERVEAU LOGISTIQUE ZIBRIDGE.
    Instancie dynamiquement les drivers CRM en injectant les credentials et la config.
    """
    
    SUPPORTED_CONNECTORS = {
        "hubspot": "HubSpot API (V3)",
        "file": "Fichiers Locaux (CSV/Excel)",
        "salesforce": "Salesforce API (Bulk Ready)",
    }
    
    @staticmethod
    def get_connector(
        crm_type: str, 
        credentials: Dict[str, Any], 
        project_id: Optional[int] = None, 
        project_config: Optional[Dict[str, Any]] = None,
        **kwargs  # 🛡️ Elon Mode : Absorbe les arguments inattendus pour éviter les crashs
    ):
        """
        🚀 Instanciation dynamique avec support du project_id et des configurations agnostiques.
        """
        source = crm_type.lower().strip()
        
        if source not in ConnectorFactory.SUPPORTED_CONNECTORS:
            raise ValueError(f"❌ La source '{crm_type}' n'est pas supportée par Zibridge.")
        
        # 1. LOGIQUE HUBSPOT
        if source == "hubspot":
            # On cherche le token dans les credentials, sinon dans l'env
            token = credentials.get("token") or os.getenv("HUBSPOT_TOKEN")
            if not token:
                raise ValueError("Token HubSpot absent des credentials et de l'environnement.")
            
            # Utilisation du project_id passé ou présent dans les credentials
            p_id = project_id  # PAS depuis credentials

            logger.debug(f"🔌 Drive HubSpot initialisé (Project ID: {p_id})")

            return HubSpotConnector(
                token=token,
                credentials=credentials,
                project_config=project_config,
                project_id=p_id
)

        
        # 2. LOGIQUE FILE (CSV/JSON/EXCEL)
        elif source == "file":
            file_path = credentials.get("file_path") or credentials.get("path")
            if not file_path:
                raise ValueError("Chemin de fichier (file_path) manquant pour le connecteur 'file'.")
            
            logger.debug(f"📂 Connecteur Fichier initialisé pour: {file_path}")
            return FileConnector(
                credentials=credentials, 
                project_config=project_config
            )

        # 3. LOGIQUE SALESFORCE
        # elif source == "salesforce":
        #    return SalesforceConnector(credentials=credentials, project_config=project_config)

        raise ValueError(f"Le driver pour '{source}' est référencé mais non implémenté.")

    @staticmethod
    def list_available_drivers() -> Dict[str, str]:
        """Utile pour peupler un Select dans le Frontend Refine v4."""
        return ConnectorFactory.SUPPORTED_CONNECTORS