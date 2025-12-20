from sqlmodel import Session, select
from src.utils.db import engine
from src.core.models import SnapshotItem
import pandas as pd # Pour un affichage propre en tableau

def audit_object(obj_type: str, ext_id: str):
    with Session(engine) as session:
        statement = select(SnapshotItem).where(
            SnapshotItem.object_type == obj_type,
            SnapshotItem.object_id == ext_id
        ).order_by(SnapshotItem.snapshot_id)
        
        results = session.exec(statement).all()
        
        data = []
        for item in results:
            data.append({
                "Snap_ID": item.snapshot_id,
                "Hash": item.content_hash[:12] + "...",
                "Type": item.object_type,
                "ID": item.object_id
            })
        
        print(f"\n--- Historique de {obj_type} ID: {ext_id} ---")
        if not data:
            print("❌ Aucun historique trouvé.")
        else:
            print(pd.DataFrame(data).to_string(index=False))

if __name__ == "__main__":
    # On vérifie ton contact modifié
    audit_object("contacts", "1")