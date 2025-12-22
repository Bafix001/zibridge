import json
from loguru import logger
from sqlmodel import Session, select
from typing import List, Dict, Any, Optional

from src.core.hashing import calculate_content_hash
from src.core.models import Blob, SnapshotItem, Snapshot
from src.utils.db import engine, storage_manager
from src.core.graph import GraphManager
from src.connectors.base import BaseConnector # ✅ On importe l'interface

class SnapshotEngine:
    def __init__(self, snapshot_id: int):
        self.snapshot_id = snapshot_id
        self.graph = GraphManager()

    def process_item(
        self, 
        connector: BaseConnector, # ✅ On passe le connecteur pour la normalisation
        object_type: str, 
        external_id: str, 
        raw_data: Dict[str, Any], 
        associations: Optional[Dict[str, List[str]]] = None
    ):
        """
        Traite et versionne un objet de manière agnostique.
        """
        # 1. Normalisation via le connecteur (CRM ou CSV)
        # Cela permet d'avoir des hashs cohérents même si les clés API changent
        data = connector.normalize_data(raw_data, object_type)
        
        # 2. Injection des liens de Suture
        if associations:
            data["_zibridge_links"] = associations
            
        # 3. Marqueur de source pour l'audit
        data["_zibridge_meta"] = {
            "snapshot_id": self.snapshot_id,
            "source_type": connector.source_type,
            "original_id": external_id
        }

        item_hash = calculate_content_hash(data)
        object_path = f"blobs/{item_hash}.json"
        
        with Session(engine) as session:
            # ✅ Content-Addressable Storage (CAS)
            # Si le hash existe déjà, on ne réécrit pas dans MinIO
            existing_blob = session.get(Blob, item_hash)
            
            if not existing_blob:
                try:
                    storage_manager.save_json(object_path, data)
                    session.add(Blob(hash=item_hash, content_type=object_type))
                except Exception as e:
                    logger.error(f"❌ Erreur stockage MinIO (Blob {item_hash}): {e}")
                    return

            # 4. Enregistrement de l'item dans le snapshot actuel
            # On ajoute le namespace pour éviter les collisions d'ID multi-sources
            new_item = SnapshotItem(
                snapshot_id=self.snapshot_id,
                object_id=external_id,
                object_type=object_type,
                content_hash=item_hash,
                source_namespace=type(connector).__name__ 
            )
            session.add(new_item)

            # 5. Mise à jour du graphe de relations
            try:
                self.graph.update_relation(self.snapshot_id, object_type, external_id, item_hash)
            except Exception as ge:
                logger.warning(f"⚠️ Graphe non mis à jour pour {external_id}: {ge}")

            session.commit()

    def get_all_items_from_minio(self, object_type: str) -> List[Dict[str, Any]]:
        """Récupère tous les objets d'un type pour le snapshot courant."""
        logger.info(f"📂 Hydratation des {object_type} depuis MinIO (Snap #{self.snapshot_id})")
        items_data = []
        
        with Session(engine) as session:
            statement = select(SnapshotItem).where(
                SnapshotItem.snapshot_id == self.snapshot_id,
                SnapshotItem.object_type == object_type
            )
            results = session.exec(statement).all()

            for item in results:
                try:
                    data = storage_manager.get_json(f"blobs/{item.content_hash}.json")
                    # On injecte l'ID technique s'il manque pour faciliter la restauration
                    if "id" not in data: data["id"] = item.object_id
                    items_data.append(data)
                except Exception as e:
                    logger.error(f"❌ Blob corrompu ou manquant {item.content_hash}: {e}")
        
        return items_data