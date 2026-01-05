import json
from typing import List, Dict, Any, Optional, Set
from sqlmodel import Session, select
from loguru import logger

from src.utils.db import engine, storage_manager
from src.core.models import SnapshotItem, Snapshot, SnapshotProject

class DiffEngine:
    def __init__(self, old_snap_id: int, new_snap_id: int, project_id: Optional[int] = None):
        self.old_id = old_snap_id
        self.new_id = new_snap_id
        self.storage = storage_manager
        self.project_config = {}
        
        if project_id:
            with Session(engine) as session:
                project = session.get(SnapshotProject, project_id)
                if project:
                    self.project_config = project.config

    def check_fast_path(self) -> bool:
        """🚀 Évite tout calcul si les snapshots sont identiques via Merkle Root."""
        with Session(engine) as session:
            old_snap = session.get(Snapshot, self.old_id)
            new_snap = session.get(Snapshot, self.new_id)
            if old_snap and new_snap and old_snap.root_hash == new_snap.root_hash:
                return True
        return False

    def _get_inventory_stream(self, snap_id: int):
        """Récupère l'inventaire via un générateur pour économiser la RAM."""
        with Session(engine) as session:
            # Utilisation de execution_options pour le streaming SQL
            statement = select(SnapshotItem).where(SnapshotItem.snapshot_id == snap_id)
            for item in session.exec(statement):
                yield f"{item.object_type}/{item.object_id}", item.content_hash

    def generate_detailed_report(self):
        """🔥 RAPPORT DE DIFFÉRENCE OPTIMISÉ."""
        if self.check_fast_path():
            return {
                "summary": {
                    "created": 0,
                    "updated": 0,
                    "deleted": 0,
                    "unchanged": 0,
                    "total_changes": 0
                },
                "details": {
                    "created": [],
                    "updated": [],
                    "deleted": []
                },
                "status": "identical"
            }


        # On transforme le stream en dict pour la comparaison (nécessaire pour old_map)
        # Pour les très gros volumes (>1M), on utiliserait une table tempo SQL
        old_map = dict(self._get_inventory_stream(self.old_id))
        
        report = {
            "summary": {"created": 0, "updated": 0, "deleted": 0, "unchanged": 0},
            "details": {"created": [], "updated": [], "deleted": []}
        }

        # Passage unique sur le nouveau snapshot
        new_keys_seen = set()
        for key, new_hash in self._get_inventory_stream(self.new_id):
            new_keys_seen.add(key)
            obj_type, obj_id = key.split('/', 1)
            
            if key not in old_map:
                report["summary"]["created"] += 1
                report["details"]["created"].append({"type": obj_type, "id": obj_id})
            elif old_map[key] != new_hash:
                report["summary"]["updated"] += 1
                # On ne compare les détails que si nécessaire (Lazy Loading)
                report["details"]["updated"].append({
                    "type": obj_type, 
                    "id": obj_id, 
                    "old_hash": old_map[key], 
                    "new_hash": new_hash
                })
            else:
                report["summary"]["unchanged"] += 1

        # Détection des suppressions
        for key in old_map:
            if key not in new_keys_seen:
                obj_type, obj_id = key.split('/', 1)
                report["summary"]["deleted"] += 1
                report["details"]["deleted"].append({"type": obj_type, "id": obj_id})

        return report

    def get_diff_detail(self, obj_type: str, obj_id: str, old_hash: str, new_hash: str):
        """
        🔍 ANALYSE DE PRÉCISION (Appelée à la demande dans Refine).
        Compare deux JSON pour l'affichage côte à côte.
        """
        old_data = self.storage.get_json(f"blobs/{old_hash}.json")
        new_data = self.storage.get_json(f"blobs/{new_hash}.json")
        
        old_props = old_data.get("properties", old_data)
        new_props = new_data.get("properties", new_data)
        
        diff = {}
        all_keys = set(old_props.keys()) | set(new_props.keys())
        
        for k in all_keys:
            if k.startswith("_"): continue
            v1, v2 = old_props.get(k), new_props.get(k)
            if str(v1) != str(v2):
                diff[k] = {"old": v1, "new": v2}
        
        return diff