from sqlmodel import Session, select
from src.utils.db import engine
from src.core.models import SnapshotItem

def show_items():
    with Session(engine) as session:
        statement = select(SnapshotItem)
        results = session.exec(statement).all()
        
        print(f"{'ID':<5} | {'Snap_ID':<8} | {'Ext_ID':<10} | {'Hash (début)':<15}")
        print("-" * 50)
        for item in results:
            print(f"{item.id:<5} | {item.snapshot_id:<8} | {item.object_id:<10} | {item.content_hash[:12]}...")


# Ajoute ceci à la fin de ton check_db.py pour tester
def check_versions_of_contact_1():
    with Session(engine) as session:
        statement = select(SnapshotItem).where(
            SnapshotItem.object_type == "contacts",
            SnapshotItem.object_id == "1"
        )
        results = session.exec(statement).all()
        for item in results:
            print(f"Snap: {item.snapshot_id} | Hash: {item.content_hash[:8]}...")

check_versions_of_contact_1()            

if __name__ == "__main__":
    show_items()