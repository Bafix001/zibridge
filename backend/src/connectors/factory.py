from typing import Dict, Any
from src.connectors.hubspot import HubSpotConnector
from src.connectors.csv_connector import CSVConnector
from loguru import logger

class ConnectorFactory:
    """
    Le cerveau logistique de Zibridge. 
    Il transforme une intention du Frontend en un objet Connecteur concret.
    """
    
    @staticmethod
    def get_connector(crm_type: str, credentials: Dict[str, Any]):
        source = crm_type.lower().strip()
        
        # 1. Connecteurs API (SaaS)
        if source == "hubspot":
            return HubSpotConnector(token=credentials.get("token"))
        
        # 2. Connecteurs Fichiers (Agnosticisme Local)
        elif source == "csv":
            # On récupère soit le chemin direct du fichier, soit le dossier
            # ✅ On priorise 'file_path' pour l'agnosticisme total
            file_path = credentials.get("file_path")
            data_dir = credentials.get("data_dir", "data/uploads")
            
            logger.info(f"🔌 Initialisation Connecteur CSV | Source : {file_path or data_dir}")
            
            return CSVConnector(credentials={
                "file_path": file_path,
                "data_dir": data_dir
            })
            
        else:
            raise ValueError(f"La source '{crm_type}' n'est pas supportée.")