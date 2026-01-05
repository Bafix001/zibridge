from typing import List, Optional, Dict, Any
from loguru import logger
from sqlmodel import Session
from src.core.models import SnapshotProject, IdMapping
from src.utils.db import engine
from src.core.snapshot import SnapshotEngine
from src.core.hashing import calculate_content_hash
from src.core.graph import GraphManager
from rich.console import Console

console = Console()

class RestoreEngine:
    def __init__(self, project_id: int, connector: Any, snapshot_id: Optional[int] = None, dry_run: bool = False):
        self.project_id = project_id
        self.snapshot_id = snapshot_id
        self.connector = connector
        self.dry_run = dry_run
        self.graph = GraphManager()
        
        if snapshot_id:
            self.snap_engine = SnapshotEngine(snapshot_id=snapshot_id)
        
        with Session(engine) as session:
            self.project = session.get(SnapshotProject, project_id)

    def run(self) -> Dict[str, int]:
        """🚀 Restauration batch optimisée : Création, Batch Update et Suture."""
        report = {"success": 0, "failed": 0, "ignored": 0, "updates": 0, "sutures": 0,
                  "to_create": 0, "to_update": 0, "to_suture": 0}
        id_translation_map = {}
        all_pending_links = []

        priority_order = self.graph.get_restoration_order(self.project_id)
        if not priority_order:
            priority_order = self.connector.get_available_object_types()

        for obj_type in priority_order:
            logger.info(f"⚡ Analyse chirurgicale : {obj_type}")

            # Récupère tous les items du snapshot
            try:
                target_items = self.snap_engine.get_all_items(obj_type)
            except AttributeError:
                logger.warning(f"⚠️ SnapshotEngine n'a pas get_all_items, on skip {obj_type}")
                continue

            if not target_items:
                continue

            # État actuel depuis le connecteur
            current_items = {str(item['id']): item for item in self.connector.extract_data(obj_type)}

            to_create_props, to_create_old_ids = [], []
            to_update_batch = []

            for target_item in target_items:
                old_id = str(target_item.get("id"))
                current_item = current_items.get(old_id)

                target_props = target_item.get("properties", target_item)
                target_hash = calculate_content_hash(target_item)

                # --- Création ou Update ---
                if not current_item:
                    to_create_props.append(target_props)
                    to_create_old_ids.append(old_id)
                    report["to_create"] += 1
                else:
                    actual_crm_id = str(current_item['id'])
                    id_translation_map[old_id] = actual_crm_id
                    current_hash = calculate_content_hash(current_item)
                    if current_hash != target_hash:
                        to_update_batch.append({"id": actual_crm_id, "properties": target_props})
                        report["to_update"] += 1
                    else:
                        report["ignored"] += 1

                # --- Collecte des liens pour suture ---
                snap_links = target_item.get("_zibridge_links", [])
                current_links = current_item.get("_zibridge_links", []) if current_item else []

                normalized_snap_links = [
                    l if isinstance(l, dict) and "id" in l and "type" in l else {"id": l, "type": "unknown"}
                    for l in snap_links
                ]
                normalized_current_links = [
                    l if isinstance(l, dict) and "id" in l and "type" in l else {"id": l, "type": "unknown"}
                    for l in current_links
                ]

                existing_link_keys = {f"{l['type']}:{l['id']}" for l in normalized_current_links}

                for link in normalized_snap_links:
                    link_key = f"{link['type']}:{link['id']}"
                    if link_key not in existing_link_keys:
                        all_pending_links.append({
                            "source_type": obj_type,
                            "source_old_id": old_id,
                            "target_type": link["type"],
                            "target_old_id": str(link["id"])
                        })
                        report["to_suture"] += 1

            # --- Batch Creation ---
            if to_create_props and not self.dry_run:
                new_objects = self.connector.batch_push_upsert(obj_type, to_create_props)
                for i, new_obj in enumerate(new_objects):
                    o_id = to_create_old_ids[i]
                    n_id = str(new_obj["id"])
                    id_translation_map[o_id] = n_id
                    self._save_id_mapping(obj_type, o_id, n_id)
                report["success"] += len(new_objects)

            # --- Batch Update (HubSpot Mode) ---
            if to_update_batch and not self.dry_run:
                for i in range(0, len(to_update_batch), 100):
                    chunk = to_update_batch[i:i+100]
                    try:
                        self.connector.batch_update(obj_type, chunk)  # HubSpot : batch 100 max
                    except AttributeError:
                        # fallback si batch_update non implémenté
                        for item in chunk:
                            self.connector.push_update(obj_type, item["id"], item["properties"])
                    except Exception as e:
                        logger.error(f"❌ Erreur batch update {obj_type}: {e}")

        # --- Suture globale sécurisée ---
        if not self.dry_run and all_pending_links:
            logger.info(f"🔗 Suture de {len(all_pending_links)} relations...")
            self._execute_translated_suture_batch_safe(all_pending_links, id_translation_map)
            report["sutures"] = len(all_pending_links)

        return report

    def _save_id_mapping(self, obj_type: str, old_id: str, new_id: str):
        with Session(engine) as session:
            mapping = IdMapping(
                project_id=self.project_id,
                object_type=obj_type,
                source_system=self.connector.source_type,
                old_id=old_id,
                new_id=new_id
            )
            session.add(mapping)
            session.commit()

    def _execute_translated_suture_batch_safe(self, pending_links: List[Dict], translation_map: Dict):
        batches = {}
        for link in pending_links:
            real_source_id = translation_map.get(link["source_old_id"], link["source_old_id"])
            real_target_id = translation_map.get(link["target_old_id"], link["target_old_id"])
            key = (link["source_type"], link["target_type"])
            # ⚡ Ajout des types pour que HubSpot ait from_type/to_type
            batches.setdefault(key, []).append({
                "from_type": link["source_type"],
                "from_id": real_source_id,
                "to_type": link["target_type"],
                "to_id": real_target_id
            })

        for (s_type, t_type), associations in batches.items():
            for i in range(0, len(associations), 100):
                chunk = associations[i:i+100]
                try:
                    self.connector.batch_create_associations(chunk)
                except Exception as e:
                    logger.error(f"❌ Erreur lors de la suture batch {s_type}->{t_type} : {e}")

    def get_preflight_report(self) -> Dict[str, int]:
        """Analyse d'impact avant exécution (simulation)."""
        report = {"to_create": 0, "to_update": 0, "to_suture": 0, "ignored": 0}
        if not hasattr(self, "snap_engine"):
            return report

        priority_order = self.graph.get_restoration_order(self.project_id)
        if not priority_order:
            priority_order = self.connector.get_available_object_types()

        for obj_type in priority_order:
            try:
                target_items = self.snap_engine.get_all_items(obj_type)
            except AttributeError:
                continue
            current_items = {str(item['id']): item for item in self.connector.extract_data(obj_type)}

            for target_item in target_items:
                old_id = str(target_item.get("id"))
                current_item = current_items.get(old_id)
                target_hash = calculate_content_hash(target_item)

                if not current_item:
                    report["to_create"] += 1
                else:
                    current_hash = calculate_content_hash(current_item)
                    if current_hash != target_hash:
                        report["to_update"] += 1
                    else:
                        report["ignored"] += 1

                snap_links = target_item.get("_zibridge_links", [])
                current_links = current_item.get("_zibridge_links", []) if current_item else []

                normalized_snap_links = [
                    l if isinstance(l, dict) and "id" in l and "type" in l else {"id": l, "type": "unknown"}
                    for l in snap_links
                ]
                normalized_current_links = [
                    l if isinstance(l, dict) and "id" in l and "type" in l else {"id": l, "type": "unknown"}
                    for l in current_links
                ]

                existing_link_keys = {f"{l['type']}:{l['id']}" for l in normalized_current_links}
                for link in normalized_snap_links:
                    link_key = f"{link['type']}:{link['id']}"
                    if link_key not in existing_link_keys:
                        report["to_suture"] += 1

        return report

    def display_preflight(self, analysis: Dict[str, int]):
        console.print("\n🔍 Analyse préflight :")
        console.print(f"⚡ À créer : {analysis['to_create']}")
        console.print(f"📝 À mettre à jour : {analysis['to_update']}")
        console.print(f"🔗 À suture : {analysis['to_suture']}")
        console.print(f"😴 Ignorés : {analysis['ignored']}")
