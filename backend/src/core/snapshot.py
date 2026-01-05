import json
from loguru import logger
from sqlmodel import Session, select
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.core.hashing import calculate_content_hash, calculate_merkle_root
from src.core.models import Blob, NormalizedStorage, SnapshotItem, Snapshot, SnapshotProject
from src.utils.db import engine, storage_manager
from src.core.graph import GraphManager
from src.connectors.base import BaseConnector 

class SnapshotEngine:
    def __init__(self, snapshot_id: int):
        self.snapshot_id = snapshot_id
        self.graph = GraphManager()
        self._batch_items = []
        self._all_hashes = []  # Pour calculer le Merkle Root final

    def process_item(
        self, 
        connector: BaseConnector, 
        object_type: str, 
        external_id: str, 
        raw_data: Dict[str, Any], 
        associations: Optional[Dict[str, Any]] = None,
        commit_now: bool = False
    ):
        """
        ⚡ Traitement ultra-rapide avec capture de relations.
        """
        # 1. Normalisation technique
        data = connector.normalize_data(raw_data, object_type)
        
        # 2. Capture des associations (Suture Chirurgicale)
        if associations:
            data["_zibridge_links"] = associations
            for target_type, target_ids in associations.items():
                ids_list = target_ids if isinstance(target_ids, list) else [target_ids]
                for t_id in ids_list:
                    self.graph.create_relation(
                        from_id=external_id,
                        from_type=object_type,
                        to_id=str(t_id),
                        to_type=target_type,
                        rel_type="ASSOCIATED_WITH",
                        project_id=getattr(connector, 'project_id', None)
                    )
                
        # 3. Hashing
        item_hash = calculate_content_hash(data)
        self._all_hashes.append(item_hash)
        
        # 4. Stockage Blob (Dédoublonnage)
        with Session(engine) as session:
            existing_blob = session.get(Blob, item_hash)
            if not existing_blob:
                storage_manager.save_json(f"blobs/{item_hash}.json", data)
                session.add(Blob(hash=item_hash, content_type=object_type))
                session.commit()

        # 5. Préparation du Batch SQL
        self._batch_items.append(
            SnapshotItem(
                snapshot_id=self.snapshot_id,
                object_id=external_id,
                object_type=object_type,
                content_hash=item_hash,
                source_namespace=type(connector).__name__ 
            )
        )

        if len(self._batch_items) >= 500 or commit_now:
            self._flush_batch()

    def _flush_batch(self):
        """Flush batch SQL en masse pour la performance."""
        if not self._batch_items:
            return
            
        with Session(engine) as session:
            try:
                session.bulk_save_objects(self._batch_items)
                session.commit()
                self._batch_items = []
            except Exception as e:
                logger.error(f"❌ Erreur Bulk Flush: {e}")
                session.rollback()

    def finalize_snapshot(self):
        """Calcule le Merkle Root final et marque le snapshot comme terminé."""
        root_hash = calculate_merkle_root(self._all_hashes)
        with Session(engine) as session:
            snap = session.get(Snapshot, self.snapshot_id)
            if snap:
                snap.root_hash = root_hash
                snap.status = "completed"
                session.add(snap)
                session.commit()
        logger.success(f"🏁 Snapshot {self.snapshot_id} finalisé (Merkle: {root_hash[:10]}...)")

    def merge_to_master(self, project_id: int, object_type: str):
        """Fusionne les données du snapshot vers le Master Storage."""
        logger.info(f"🔄 Merge vers Master Storage : {object_type}")
        with Session(engine) as session:
            project = session.get(SnapshotProject, project_id)
            mapping_config = project.config.get("mappings", {}).get(object_type, {})
            unique_field = mapping_config.get("unique_id", "id")

            statement = select(SnapshotItem).where(
                SnapshotItem.snapshot_id == self.snapshot_id,
                SnapshotItem.object_type == object_type
            )
            items = session.exec(statement).all()

            for item in items:
                raw_data = storage_manager.get_json(f"blobs/{item.content_hash}.json")
                global_id = str(raw_data.get("properties", raw_data).get(unique_field, item.object_id))

                master_record = session.exec(
                    select(NormalizedStorage).where(
                        NormalizedStorage.project_id == project_id,
                        NormalizedStorage.object_type == object_type,
                        NormalizedStorage.global_id == global_id
                    )
                ).first()

                if not master_record:
                    master_record = NormalizedStorage(
                        project_id=project_id,
                        object_type=object_type,
                        global_id=global_id,
                        data=raw_data,
                        last_snapshot_id=self.snapshot_id
                    )
                    session.add(master_record)
                else:
                    master_record.data = raw_data
                    master_record.last_snapshot_id = self.snapshot_id
                    master_record.updated_at = datetime.utcnow()
                
            session.commit()

    def get_items(self, object_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retourne tous les items du snapshot, optionnellement filtrés par type."""
        items = []
        with Session(engine) as session:
            statement = select(SnapshotItem).where(SnapshotItem.snapshot_id == self.snapshot_id)
            if object_type:
                statement = statement.where(SnapshotItem.object_type == object_type)
            for si in session.exec(statement):
                data = storage_manager.get_json(f"blobs/{si.content_hash}.json")
                items.append({
                    "id": si.object_id,
                    "object_type": si.object_type,
                    **data
                })
        return items

    def get_all_items(self, object_type: str) -> List[Dict[str, Any]]:
        """Retourne tous les items stockés pour un type d'objet."""
        items = []
        with Session(engine) as session:
            statement = select(SnapshotItem).where(
                SnapshotItem.snapshot_id == self.snapshot_id,
                SnapshotItem.object_type == object_type
            )
            results = session.exec(statement).all()
            for item in results:
                data = storage_manager.get_json(f"blobs/{item.content_hash}.json")
                items.append({
                    "id": item.object_id,
                    "object_type": item.object_type,
                    **data
                })
        return items
