from sqlmodel import Session, select
from src.utils.db import engine
from src.core.models import SnapshotItem
from loguru import logger

class DiffEngine:
    def __init__(self, old_snap_id: int, new_snap_id: int):
        self.old_id = old_snap_id
        self.new_id = new_snap_id

    def _get_inventory(self, snap_id: int):
        """
        Récupère tous les objets d'un snapshot.
        Retourne un dictionnaire : { "type/id": "hash_du_contenu" }
        """
        with Session(engine) as session:
            statement = select(SnapshotItem).where(SnapshotItem.snapshot_id == snap_id)
            items = session.exec(statement).all()
            # On utilise le format 'type/id' comme clé unique
            return {f"{i.object_type}/{i.object_id}": i.content_hash for i in items}

    def generate_report(self):
        """ Calcule les différences entre les deux inventaires. """
        old_map = self._get_inventory(self.old_id)
        new_map = self._get_inventory(self.new_id)

        report = {
            "created": [],
            "updated": [],
            "deleted": [],
            "unchanged_count": 0
        }

        # 1. Analyse des ajouts et des modifications (présents dans le nouveau)
        for key, new_hash in new_map.items():
            obj_type, obj_id = key.split('/')
            
            if key not in old_map:
                # C'est une création
                report["created"].append({
                    "type": obj_type, 
                    "id": obj_id, 
                    "hash": new_hash
                })
            
            elif old_map[key] != new_hash:
                # C'est une modification : on garde les DEUX hashes pour comparer le contenu plus tard
                report["updated"].append({
                    "type": obj_type, 
                    "id": obj_id, 
                    "old_hash": old_map[key], 
                    "new_hash": new_hash
                })
            else:
                # Rien n'a changé
                report["unchanged_count"] += 1

        # 2. Analyse des suppressions (présents dans l'ancien mais plus dans le nouveau)
        for key, old_hash in old_map.items():
            if key not in new_map:
                obj_type, obj_id = key.split('/')
                report["deleted"].append({
                    "type": obj_type, 
                    "id": obj_id, 
                    "hash": old_hash
                })

        return report