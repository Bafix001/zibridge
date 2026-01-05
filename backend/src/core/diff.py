import json
from typing import List, Dict, Any, Optional
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
        with Session(engine) as session:
            old_snap = session.get(Snapshot, self.old_id)
            new_snap = session.get(Snapshot, self.new_id)
            return old_snap and new_snap and old_snap.root_hash == new_snap.root_hash

    def _get_inventory_stream(self, snap_id: int):
        with Session(engine) as session:
            statement = select(SnapshotItem).where(SnapshotItem.snapshot_id == snap_id)
            for item in session.exec(statement):
                yield f"{item.object_type}/{item.object_id}", item.content_hash

    def generate_detailed_report(self):
        if self.check_fast_path():
            return {
                "summary": {"created": 0, "updated": 0, "deleted": 0, "unchanged": 0, "total_changes": 0},
                "details": {"created": [], "updated": [], "deleted": []},
                "status": "identical"
            }

        old_map = dict(self._get_inventory_stream(self.old_id))
        new_map = dict(self._get_inventory_stream(self.new_id))
        
        report = {
            "summary": {"created": 0, "updated": 0, "deleted": 0, "unchanged": 0},
            "details": {"created": [], "updated": [], "deleted": []}
        }

        # 1. Objets créés et modifiés
        for key, new_hash in new_map.items():
            obj_type, obj_id = key.split('/', 1)
            
            if key not in old_map:
                # Objet créé
                report["summary"]["created"] += 1
                report["details"]["created"].append({
                    "type": obj_type, 
                    "id": obj_id,
                    "hash": new_hash
                })
            else:
                old_hash = old_map[key]
                if old_hash != new_hash:
                    # Objet modifié - analyse détaillée
                    deep_diff = self.get_diff_detail(obj_type, obj_id, old_hash, new_hash)
                    
                    has_prop_changes = bool(deep_diff.get("properties"))
                    rels = deep_diff.get("relations", {})
                    has_rel_changes = bool(rels.get("removed") or rels.get("added"))

                    if has_prop_changes or has_rel_changes:
                        report["summary"]["updated"] += 1
                        report["details"]["updated"].append({
                            "type": obj_type,
                            "id": obj_id,
                            "old_hash": old_hash,
                            "new_hash": new_hash,
                            "diff": deep_diff
                        })
                    else:
                        report["summary"]["unchanged"] += 1
                else:
                    report["summary"]["unchanged"] += 1

        # 2. Objets supprimés ⚡ FIX CRITIQUE
        for key, old_hash in old_map.items():
            if key not in new_map:
                obj_type, obj_id = key.split('/', 1)
                
                # Récupération des données de l'ancien objet pour afficher les relations perdues
                old_data = self.storage.get_json(f"blobs/{old_hash}.json")
                old_links = self._normalize_links(old_data.get("_zibridge_links", {}))
                
                report["summary"]["deleted"] += 1
                report["details"]["deleted"].append({
                    "type": obj_type,
                    "id": obj_id,
                    "old_hash": old_hash,
                    "lost_relations": old_links  # Relations perdues avec l'objet
                })

        report["summary"]["total_changes"] = (
            report["summary"]["created"] + 
            report["summary"]["updated"] + 
            report["summary"]["deleted"]
        )
        
        return report
    
    def get_diff_detail(self, obj_type: str, obj_id: str, old_hash: str, new_hash: str):
        """🔍 Analyse les propriétés ET les relations (Sutures)."""
        old_data = self.storage.get_json(f"blobs/{old_hash}.json")
        new_data = self.storage.get_json(f"blobs/{new_hash}.json")
        
        diff = {"properties": {}, "relations": {"added": [], "removed": []}}
        
        # 1. Comparaison des propriétés métier
        old_props = old_data.get("properties", old_data)
        new_props = new_data.get("properties", new_data)
        all_keys = set(old_props.keys()) | set(new_props.keys())
        
        for k in all_keys:
            if k.startswith("_"): 
                continue
            v1, v2 = old_props.get(k), new_props.get(k)
            if str(v1) != str(v2):
                diff["properties"][k] = {"old": v1, "new": v2}
        
        # 2. Comparaison des RELATIONS ⚡
        old_links = old_data.get("_zibridge_links", {})
        new_links = new_data.get("_zibridge_links", {})
        
        old_set = self._normalize_links(old_links)
        new_set = self._normalize_links(new_links)
        
        # Calcul des deltas de relations
        removed = old_set - new_set
        added = new_set - old_set
        
        diff["relations"]["removed"] = sorted(list(removed))
        diff["relations"]["added"] = sorted(list(added))
        
        return diff
    
    def _normalize_links(self, links: Any) -> set:
        """
        🔗 Normalise les liens vers un set de strings 'type:id' pour comparaison.
        Supporte: dict, list[dict], list[str]
        """
        result = set()
        
        if not links:
            return result
        
        # Format dict (votre format principal)
        if isinstance(links, dict):
            for rel_type, ids in links.items():
                id_list = ids if isinstance(ids, list) else [ids]
                for rel_id in id_list:
                    result.add(f"{rel_type}:{rel_id}")
        
        # Format list[dict]
        elif isinstance(links, list):
            for item in links:
                if isinstance(item, dict) and "id" in item:
                    rel_type = item.get("type", "unknown")
                    result.add(f"{rel_type}:{item['id']}")
                elif isinstance(item, str):
                    # Déjà au format "type:id"
                    result.add(item)
        
        return result