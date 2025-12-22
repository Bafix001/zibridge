from src.connectors.csv_connector import CSVConnector
from src.core.snapshot import SnapshotEngine
from src.core.models import Snapshot
from src.utils.db import engine
from sqlmodel import Session

def test_drive():
    # 1. On utilise le connecteur CSV au lieu de HubSpot
    connector = CSVConnector(data_dir="data/test_samples")
    
    with Session(engine) as session:
        snap = Snapshot(source="CSV_Import", source_type="file", status="running")
        session.add(snap)
        session.commit()
        session.refresh(snap)

        # 2. Le SnapshotEngine travaille sans savoir que c'est du CSV
        engine_snap = SnapshotEngine(snapshot_id=snap.id)
        
        for item in connector.extract_data("contacts"):
            engine_snap.process_item(
                connector=connector,
                object_type="contacts",
                external_id=str(item['id']),
                raw_data=item
            )
    print(f"✅ Snapshot #{snap.id} terminé avec succès depuis un CSV !")

if __name__ == "__main__":
    test_drive()