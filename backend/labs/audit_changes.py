import json
from sqlmodel import Session, select
from src.utils.db import engine, minio_client
from src.core.models import SnapshotItem
from src.utils.config import settings
from src.core.diff import DiffEngine
from loguru import logger

def get_content_from_minio(content_hash):
    """Récupère le JSON dans MinIO via son hash"""
    try:
        response = minio_client.get_object(
            settings.minio.bucket,
            f"blobs/{content_hash}.json"
        )
        return json.loads(response.read().decode('utf-8'))
    except Exception:
        return None

def audit_all_modifications(start_snap, end_snap):
    # 1. Identifier quels IDs ont bougé
    diff = DiffEngine(start_snap, end_snap)
    report = diff.generate_report()
    modified_keys = report['updated']

    print(f"🔎 Analyse détaillée des {len(modified_keys)} modifications...")

    with Session(engine) as session:
        for key in modified_keys:
            obj_type, ext_id = key.split('/')
            
            # 2. Récupérer le Hash de la version Baseline (Début)
            old_item = session.exec(
                select(SnapshotItem).where(
                    SnapshotItem.snapshot_id == start_snap,
                    SnapshotItem.object_id == ext_id
                )
            ).first()

            # 3. Récupérer le Hash de la version Actuelle (Fin)
            new_item = session.exec(
                select(SnapshotItem).where(
                    SnapshotItem.snapshot_id == end_snap,
                    SnapshotItem.object_id == ext_id
                )
            ).first()

            if old_item and new_item:
                old_data = get_content_from_minio(old_item.content_hash)
                new_data = get_content_from_minio(new_item.content_hash)

                print(f"\n{"="*60}")
                print(f"📝 OBJET : {obj_type.upper()} ID: {ext_id}")
                print(f"{"="*60}")
                
                # On affiche uniquement les champs qui ont changé pour plus de clarté
                print(f"{'CHAMP':<15} | {'ANCIENNE VALEUR':<20} | {'NOUVELLE VALEUR'}")
                print("-" * 60)
                
                for field in old_data.keys():
                    val_old = old_data.get(field)
                    val_new = new_data.get(field)
                    if val_old != val_new:
                        print(f"{field:<15} | {str(val_old):<20} | {str(val_new)}")

if __name__ == "__main__":
    # On utilise tes Snapshots validés (8 et 16)
    audit_all_modifications(8, 16)