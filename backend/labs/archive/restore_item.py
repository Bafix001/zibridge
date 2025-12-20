import json
from sqlmodel import Session, select
from src.utils.db import engine, minio_client
from src.core.models import SnapshotItem
from src.utils.config import settings
from loguru import logger

def restore_item(obj_type: str, ext_id: str, snap_id: int):
    # 1. Trouver le hash dans Postgres
    with Session(engine) as session:
        statement = select(SnapshotItem).where(
            SnapshotItem.object_type == obj_type,
            SnapshotItem.object_id == ext_id,
            SnapshotItem.snapshot_id == snap_id
        )
        item = session.exec(statement).first()
        
        if not item:
            logger.error(f"❌ Aucune version trouvée pour {obj_type} {ext_id} au snap {snap_id}")
            return

        hash_to_get = item.content_hash
        logger.info(f"🔍 Récupération du hash {hash_to_get[:12]}...")

    # 2. Récupérer le contenu dans MinIO
    try:
        bucket = settings.minio.bucket
        
        # --- BLOC DE SÉCURITÉ ---
        if not minio_client.bucket_exists(bucket):
            logger.warning(f"🪣 Le bucket {bucket} n'existe pas. Création en cours...")
            minio_client.make_bucket(bucket)
        # ------------------------

        response = minio_client.get_object(
            bucket,
            f"blobs/{hash_to_get}.json" 
        )
        data = json.loads(response.read().decode('utf-8'))
        
        print(f"\n✅ DONNÉES RESTAURÉES (Snapshot {snap_id}) :")
        print(json.dumps(data, indent=4, ensure_ascii=False))
        return data
        
    except Exception as e:
        logger.error(f"❌ Erreur MinIO : {e}")

if __name__ == "__main__":
    # Testons les deux versions !
    print("--- VERSION ANCIENNE (Snap 11) ---")
    restore_item("contacts", "1", 11)
    
    print("\n--- VERSION ACTUELLE (Snap 12) ---")
    restore_item("contacts", "1", 12)