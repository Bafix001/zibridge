import json
from sqlmodel import Session, select
from src.utils.db import engine, storage_manager
from src.core.models import SnapshotItem
from loguru import logger


class DiffEngine:
    def __init__(self, old_snap_id: int, new_snap_id: int):
        self.old_id = old_snap_id
        self.new_id = new_snap_id
        
        # ✅ AGNOSTIQUE : Patterns de nommage universels
        self.DISPLAY_NAMES = {
            "company": ["name", "nom", "company_name", "entreprise_nom"],
            "contact": ["firstname", "lastname", "first_name", "last_name", "prenom", "nom"],
            "deal": ["dealname", "name", "title", "nom", "deal_name"],
            "ticket": ["subject", "title", "name", "objet"],
            # ✅ Fallback pour custom types
            "default": ["name", "nom", "title", "id"]
        }

    def _get_display_name(self, obj_type: str, data: dict) -> str:
        """🔥 NOM LISIBLE AGNOSTIQUE pour TOUS les types."""
        props = data.get("properties", data)
        
        # Patterns spécifiques par type
        patterns = self.DISPLAY_NAMES.get(obj_type.rstrip('s'), self.DISPLAY_NAMES["default"])
        
        for pattern in patterns:
            if pattern in props and props[pattern]:
                return str(props[pattern])[:50]  # Troncature
        
        # Fallback ultime
        return obj_type.upper()[:10] + "-ID"

    def _get_inventory(self, snap_id: int):
        """Récupère tous les objets d'un snapshot (agnostique)."""
        with Session(engine) as session:
            statement = select(SnapshotItem).where(SnapshotItem.snapshot_id == snap_id)
            items = session.exec(statement).all()
            return {f"{i.object_type}/{i.object_id}": i.content_hash for i in items}

    def generate_report(self):
        """Calcule les différences brutes (hashes) entre deux snapshots."""
        old_map = self._get_inventory(self.old_id)
        new_map = self._get_inventory(self.new_id)

        report = {
            "created": [],
            "updated": [],
            "deleted": [],
            "unchanged_count": 0
        }

        # 1. Analyse des créations et modifications
        for key, new_hash in new_map.items():
            obj_type, obj_id = key.split('/')
            
            if key not in old_map:
                report["created"].append({"type": obj_type, "id": obj_id, "hash": new_hash})
            elif old_map[key] != new_hash:
                report["updated"].append({
                    "type": obj_type, 
                    "id": obj_id, 
                    "old_hash": old_map[key], 
                    "new_hash": new_hash
                })
            else:
                report["unchanged_count"] += 1

        # 2. Analyse des suppressions
        for key, old_hash in old_map.items():
            if key not in new_map:
                obj_type, obj_id = key.split('/')
                report["deleted"].append({"type": obj_type, "id": obj_id, "hash": old_hash})

        return report

    def generate_detailed_report(self):
        """🔥 RAPPORT AGNOSTIQUE COMPLET pour Frontend."""
        raw_report = self.generate_report()
        
        detailed_report = {
            "summary": {
                "created": len(raw_report["created"]),
                "updated": len(raw_report["updated"]),
                "deleted": len(raw_report["deleted"]),
                "unchanged": raw_report["unchanged_count"]
            },
            "details": {
                "created": [],
                "updated": [],
                "deleted": []
            }
        }

        # ✅ CRÉATIONS (avec nom lisible)
        for c in raw_report["created"]:
            try:
                data = storage_manager.get_json(f"blobs/{c['hash']}.json")
                detailed_report["details"]["created"].append({
                    **c,
                    "display_name": self._get_display_name(c["type"], data)
                })
            except Exception as e:
                logger.error(f"Erreur création {c['type']}/{c['id']}: {e}")
                detailed_report["details"]["created"].append({
                    **c, "display_name": f"{c['type']}/{c['id'][:8]}"
                })

        # ✅ SUPPRESSIONS (avec nom lisible)
        for d in raw_report["deleted"]:
            try:
                data = storage_manager.get_json(f"blobs/{d['hash']}.json")
                detailed_report["details"]["deleted"].append({
                    **d,
                    "display_name": self._get_display_name(d["type"], data)
                })
            except Exception:
                detailed_report["details"]["deleted"].append({
                    **d, "display_name": f"{d['type']}/{d['id'][:8]}"
                })

        # 🔥 MISES À JOUR DÉTAILLÉES (comparaison prop par prop)
        for item in raw_report["updated"]:
            try:
                old_data = storage_manager.get_json(f"blobs/{item['old_hash']}.json")
                new_data = storage_manager.get_json(f"blobs/{item['new_hash']}.json")
                
                old_props = old_data.get("properties", old_data)
                new_props = new_data.get("properties", new_data)
                
                changes = {}
                all_keys = set(old_props.keys()) | set(new_props.keys())
                
                # ✅ FILTRES AGNOSTIQUES (dates/timestamps génériques)
                ignored_patterns = [
                    "lastmodified", "modified", "updated", "created", 
                    "hs_object_id", "object_id", "id", "_zibridge"
                ]
                
                for key in all_keys:
                    # Ignore les timestamps génériques
                    if any(pattern in key.lower() for pattern in ignored_patterns):
                        continue
                    
                    old_val = old_props.get(key)
                    new_val = new_props.get(key)
                    
                    if old_val != new_val and str(old_val) != str(new_val):
                        changes[key] = {
                            "old": old_val,
                            "new": new_val,
                            "type": type(old_val).__name__ if old_val is not None else "null"
                        }
                
                # ✅ COMPARE LES LIENS (suture)
                old_links = old_data.get("_zibridge_links", {})
                new_links = new_data.get("_zibridge_links", {})
                if old_links != new_links:
                    changes["relations"] = {
                        "old": old_links,
                        "new": new_links
                    }

                if changes:
                    detailed_report["details"]["updated"].append({
                        "type": item["type"],
                        "id": item["id"],
                        "display_name": self._get_display_name(item["type"], old_data),
                        "changes": changes,
                        "change_count": len(changes)
                    })

            except Exception as e:
                logger.error(f"Erreur diff détaillé {item['type']}/{item['id']}: {e}")

        logger.success(f"✅ Rapport Diff généré: {detailed_report['summary']}")
        return detailed_report
