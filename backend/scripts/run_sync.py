import warnings
import os
from loguru import logger
from sqlmodel import Session, select
from src.utils.db import engine

# ✅ ON IMPORTE LE CONNECTEUR DÉSIRÉ (ou via une factory)
from src.connectors.hubspot import HubSpotConnector 
from src.core.snapshot import SnapshotEngine
from src.core.models import Snapshot
from src.core.graph import GraphManager

warnings.filterwarnings("ignore", message=".*OpenSSL 1.1.1+.*")

def sync_all(connector_class):
    """
    Lance une synchronisation générique.
    :param connector_class: La classe du connecteur à utiliser (HubSpotConnector, etc.)
    """
    connector = connector_class()
    crm_name = type(connector).__name__.replace("Connector", "")

    with Session(engine) as session:
        # ✅ La source devient dynamique
        new_snap = Snapshot(source=f"{crm_name}_API", status="pending")
        session.add(new_snap)
        session.commit()
        session.refresh(new_snap)
        snap_id = new_snap.id
    
    logger.info(f"🚀 SNAPSHOT #{snap_id} DÉMARRÉ | CRM: {crm_name}")
    
    success = False
    total_count = 0
    graph_mgr = GraphManager()
    engine_snap = SnapshotEngine(snapshot_id=snap_id)
    
    try:
        # ✅ On demande au connecteur quels objets il veut synchroniser
        # (Ou on garde une liste standard si tous les CRM partagent ces noms)
        objects = ["companies", "contacts", "deals", "tickets"]
        
        for obj_type in objects:
            count = 0
            logger.info(f"📥 Extraction {obj_type}...")
            
            for item in connector.extract_data(obj_type):
                # ✅ L'ID et les Relations sont maintenant extraits proprement par le connecteur
                # Chaque item renvoyé par 'extract_data' doit déjà être "propre"
                ext_id = str(item.get("id"))
                
                # Le connecteur doit avoir injecté les liens dans '_zibridge_links' 
                # lors de l'extraction (dans hubspot.py)
                rels = item.get("_zibridge_links", {})
                
                # Sauvegarde MinIO + Postgres (via ton engine agnostique)
                engine_snap.process_item(obj_type, ext_id, item, associations=rels)
                
                # Suture Neo4j (Générique par type)
                try:
                    if obj_type == "contacts" and rels.get("company_id"):
                        graph_mgr.create_belongs_to(ext_id, rels["company_id"])
                    elif obj_type == "deals":
                        graph_mgr.create_deal_relations(ext_id, rels.get("company_id"), rels.get("contact_id"))
                    elif obj_type == "tickets":
                        graph_mgr.create_ticket_relations(ext_id, rels.get("contact_id"), rels.get("company_id"))
                except Exception as ge:
                    logger.debug(f"Erreur graphe sur {ext_id}: {ge}")

                count += 1
            logger.success(f"✅ {obj_type}: {count}")
            total_count += count
        
        success = True
    except Exception as e:
        logger.error(f"❌ Erreur critique lors de la sync: {e}")
        success = False
    finally:
        with Session(engine) as session:
            db_snap = session.get(Snapshot, snap_id)
            if db_snap:
                db_snap.status = "completed" if success else "failed"
                db_snap.item_count = total_count
                session.add(db_snap)
                session.commit()
                logger.info(f"🏁 FIN: {db_snap.status.upper()} ({total_count} items)")

if __name__ == "__main__":
    # Au lancement, on choisit le connecteur. 
    # Facile à changer pour SalesforceConnector plus tard !
    sync_all(HubSpotConnector)