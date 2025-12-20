import json
from loguru import logger
from sqlmodel import Session, select

from src.core.hashing import calculate_content_hash
from src.core.models import Blob, SnapshotItem
from src.utils.db import engine, storage_manager
from src.core.graph import GraphManager

class SnapshotEngine:
    def __init__(self, snapshot_id: int):
        self.snapshot_id = snapshot_id
        self.graph = GraphManager()

    def process_item(self, object_type: str, external_id: str, data: dict, associations: list = None):
        """Stocke l'objet avec ses liens (CAS) et met à jour le graphe."""
        
        # 🔗 INJECTION DES LIENS : On enrichit la donnée avant le stockage
        if associations:
            data["_zibridge_links"] = associations
        
        item_hash = calculate_content_hash(data)
        object_path = f"blobs/{item_hash}.json"
        
        with Session(engine) as session:
            # ... (le reste de ta logique de session est bon)
            existing_blob = session.get(Blob, item_hash)
            
            if not existing_blob:
                try:
                    storage_manager.save_json(object_path, data)
                    session.add(Blob(hash=item_hash, content_type=object_type))
                except Exception as e:
                    logger.error(f"❌ Échec stockage MinIO : {e}")
                    return

            # Ajout de l'item au snapshot
            session.add(SnapshotItem(
                snapshot_id=self.snapshot_id,
                object_id=external_id,
                object_type=object_type,
                content_hash=item_hash
            ))

            # 🧠 MISE À JOUR DU GRAPHE
            # Ici, on pourrait aussi créer des relations Neo4j entre objets !
            self.graph.update_relation(self.snapshot_id, object_type, external_id, item_hash)
            session.commit()

    def get_all_items_from_minio(self, object_type: str) -> list:
        """Récupère les objets depuis MinIO pour le snapshot actuel."""
        logger.info(f"📂 Récupération des {object_type} (Snap #{self.snapshot_id})")
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
                    items_data.append(data)
                except Exception as e:
                    logger.error(f"❌ Erreur lecture blob {item.content_hash}: {e}")
        
        return items_data