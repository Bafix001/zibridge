import json
from src.utils.db import storage_manager # Ton gestionnaire MinIO

def compare_snapshots(obj_type, obj_id, snap_old_id, snap_new_id):
    # 1. On récupère le contenu depuis MinIO
    # Note: Adapter selon ta structure de dossiers dans MinIO
    old_data = storage_manager.get_json(f"{snap_old_id}/{obj_type}/{obj_id}.json")
    new_data = storage_manager.get_json(f"{snap_new_id}/{obj_type}/{obj_id}.json")

    print(f"🔍 Comparaison de {obj_type} #{obj_id} :")
    
    # 2. On compare les clés
    all_keys = set(old_data.keys()) | set(new_data.keys())
    for key in all_keys:
        val_old = old_data.get(key)
        val_new = new_data.get(key)
        
        if val_old != val_new:
            print(f"  ❌ Champ '{key}':")
            print(f"     - Avant: {val_old}")
            print(f"     + Après: {val_new}")

if __name__ == "__main__":
    # Teste avec l'ID d'un des objets modifiés (~2)
    # compare_snapshots("companies", "ID_DE_TON_ENTREPRISE", 7, 8)
    pass