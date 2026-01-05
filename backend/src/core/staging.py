from datetime import datetime
from typing import List, Dict, Any
from sqlmodel import Session, select
from loguru import logger
from src.core.models import NormalizedStorage

class DataMerger:
    def __init__(self, session: Session, project_id: int, project_config: dict):
        self.session = session
        self.project_id = project_id
        self.config = project_config
        self._batch_size = 500

    def merge_snapshot_to_master(self, object_type: str, items: List[Dict[str, Any]], snapshot_id: int):
        """
        🚀 FUSION MASSIVE (Upsert Logic) : 
        Transforme les données brutes du snapshot en 'Source de Vérité' unique.
        """
        # 1. Détermination de la clé unique agnostique
        mapping_config = self.config.get("mappings", {}).get(object_type, {})
        unique_key = mapping_config.get("unique_id", "email")
        
        logger.info(f"🔄 Fusion {object_type} vers Master (clé: {unique_key})...")

        # 2. Pré-chargement des IDs existants pour éviter le N+1 queries
        # On récupère tous les global_ids actuels du projet pour ce type
        statement = select(NormalizedStorage.global_id).where(
            NormalizedStorage.project_id == self.project_id,
            NormalizedStorage.object_type == object_type
        )
        existing_ids = set(self.session.exec(statement).all())

        count_new = 0
        count_updated = 0

        for raw_data in items:
            # Extraction des propriétés métier
            props = raw_data.get("properties", raw_data)
            global_id = str(props.get(unique_key, raw_data.get("id")))

            if not global_id or global_id == "None":
                continue

            if global_id in existing_ids:
                # UPDATE : Utilisation d'une requête ciblée
                # Note: Dans un vrai mode "Elon", on ferait un bulk update
                statement = select(NormalizedStorage).where(
                    NormalizedStorage.project_id == self.project_id,
                    NormalizedStorage.object_type == object_type,
                    NormalizedStorage.global_id == global_id
                )
                existing_record = self.session.exec(statement).first()
                if existing_record:
                    existing_record.data.update(raw_data)
                    existing_record.last_snapshot_id = snapshot_id
                    existing_record.updated_at = datetime.utcnow()
                    self.session.add(existing_record)
                    count_updated += 1
            else:
                # INSERT
                new_entry = NormalizedStorage(
                    project_id=self.project_id,
                    object_type=object_type,
                    global_id=global_id,
                    data=raw_data,
                    last_snapshot_id=snapshot_id
                )
                self.session.add(new_entry)
                existing_ids.add(global_id) # On l'ajoute au cache local
                count_new += 1

            # Commit périodique
            if (count_new + count_updated) % self._batch_size == 0:
                self.session.commit()

        self.session.commit()
        logger.success(f"✅ Merge terminé : {count_new} nouveaux, {count_updated} mis à jour.")