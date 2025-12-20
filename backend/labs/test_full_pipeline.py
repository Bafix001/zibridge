from src.connectors.rest_api import RestApiConnector
from src.core.snapshot import SnapshotEngine
from src.core.models import Snapshot
from src.utils.db import get_db_session
from loguru import logger

def run_rest_pipeline():
    # 1. On crée le snapshot en base
    with get_db_session() as session:
        new_snap = Snapshot(source="flask_crm")
        session.add(new_snap)
        session.commit()
        session.refresh(new_snap)
        snapshot_id = new_snap.id
    
    logger.info(f"📸 Snapshot réseau créé (ID: {snapshot_id})")

    # 2. On branche le connecteur REST
    connector = RestApiConnector()
    engine = SnapshotEngine(snapshot_id=snapshot_id)

    # 3. Extraction et stockage
    if connector.test_connection():
        for item in connector.extract_data("contacts"):
            engine.process_item(
                object_type="contact", 
                external_id=item["id"], 
                data=item
            )
        logger.success("🏁 Snapshot réseau terminé !")
    else:
        logger.error("❌ Le serveur Flask n'est pas allumé !")

if __name__ == "__main__":
    run_rest_pipeline()