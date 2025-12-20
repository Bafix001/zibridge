import warnings
from loguru import logger
from sqlmodel import Session

# On importe nos outils centralisés
from src.utils.db import engine, get_neo4j_session, storage_manager
from src.connectors.rest_api import RestApiConnector
from src.core.snapshot import SnapshotEngine
from src.core.diff import DiffEngine
from src.core.models import Snapshot
from src.core.graph import GraphManager

# Silence les warnings SSL sur Mac
warnings.filterwarnings("ignore", message=".*OpenSSL 1.1.1+.*")

def link_snapshots_in_graph(parent_id, child_id):
    """Lien Git-style dans Neo4j pour la lignée temporelle."""
    with get_neo4j_session() as session:
        query = """
        MERGE (p:Snapshot {snap_id: $parent_id})
        MERGE (c:Snapshot {snap_id: $child_id})
        MERGE (p)-[:NEXT]->(c)
        """
        session.run(query, parent_id=parent_id, child_id=child_id)

def extract_relations(item: dict, obj_type: str) -> dict:
    """
    Extrait les relations d'un objet HubSpot.
    
    Returns:
        {
            "company_id": "123",
            "contact_id": "456",
            "deal_id": "789"
        }
    """
    relations = {}
    
    if obj_type == "contacts":
        # Structure API HubSpot v4 : associations -> companies -> results -> [...]
        associations = item.get("associations", {})
        
        if "companies" in associations:
            companies_data = associations["companies"]
            
            # Cas 1: Format direct avec "results"
            if isinstance(companies_data, dict) and "results" in companies_data:
                results = companies_data["results"]
                if results and len(results) > 0:
                    relations["company_id"] = str(results[0]["id"])
                    logger.debug(f"✅ Contact {item['id']} → Company {relations['company_id']}")
            
            # Cas 2: Format liste directe
            elif isinstance(companies_data, list) and len(companies_data) > 0:
                relations["company_id"] = str(companies_data[0]["id"])
                logger.debug(f"✅ Contact {item['id']} → Company {relations['company_id']}")
    
    elif obj_type == "deals":
        associations = item.get("associations", {})
        
        # Deal → Company
        if "companies" in associations:
            companies_data = associations["companies"]
            if isinstance(companies_data, dict) and "results" in companies_data:
                results = companies_data["results"]
                if results and len(results) > 0:
                    relations["company_id"] = str(results[0]["id"])
            elif isinstance(companies_data, list) and len(companies_data) > 0:
                relations["company_id"] = str(companies_data[0]["id"])
        
        # Deal → Contact
        if "contacts" in associations:
            contacts_data = associations["contacts"]
            if isinstance(contacts_data, dict) and "results" in contacts_data:
                results = contacts_data["results"]
                if results and len(results) > 0:
                    relations["contact_id"] = str(results[0]["id"])
            elif isinstance(contacts_data, list) and len(contacts_data) > 0:
                relations["contact_id"] = str(contacts_data[0]["id"])
    
    elif obj_type == "tickets":
        props = item.get("properties", {})
        
        # Ticket → Contact
        contact_id = props.get("hs_ticket_contact_id")
        if contact_id:
            relations["contact_id"] = str(contact_id)
        
        # Ticket → Company
        company_id = props.get("hs_ticket_company_id")
        if company_id:
            relations["company_id"] = str(company_id)
    
    if relations:
        logger.debug(f"🔗 {obj_type}/{item['id']}: {relations}")
    
    return relations


def sync_all():
    # 1. Création du Snapshot dans Postgres
    with Session(engine) as session:
        new_snap = Snapshot(source="HubSpot_Production_API")
        session.add(new_snap)
        session.commit()
        session.refresh(new_snap)
        snap_id = new_snap.id
    
    logger.info(f"🚀 DÉMARRAGE SYNC ZIBRIDGE | ID: {snap_id}")

    # 2. Lignée temporelle Neo4j
    if snap_id > 1:
        link_snapshots_in_graph(snap_id - 1, snap_id)
        logger.info(f"🔗 Graphe : Snap {snap_id-1} -> Snap {snap_id}")

    engine_snap = SnapshotEngine(snapshot_id=snap_id)
    graph_mgr = GraphManager()
    connector = RestApiConnector()
    objects = ["companies", "contacts", "deals"]

    for obj_type in objects:
        logger.info(f"📥 Extraction : {obj_type}...")
        count = 0
        
        for item in connector.extract_data(obj_type):
            ext_id = str(item.get("id") or item.get(f"{obj_type[:-1]}Id"))
            
            # --- INTELLIGENCE : Capture et Injection des relations ---
            relations = extract_relations(item, obj_type)
            # On stocke ces relations DANS l'item pour que MinIO les garde en mémoire
            item["_zibridge_links"] = relations 

            # --- Ingestion (Stockage du JSON enrichi des liens) ---
            engine_snap.process_item(obj_type, ext_id, item)

            # --- Mise à jour du Graphe Neo4j ---
            if obj_type == "contacts" and "company_id" in relations:
                graph_mgr.create_belongs_to(ext_id, relations["company_id"])
                logger.debug(f"🔗 Contact #{ext_id} → Company #{relations['company_id']}")
            
            elif obj_type == "deals":
                graph_mgr.create_deal_relations(
                    deal_id=ext_id,
                    company_id=relations.get("company_id"),
                    contact_id=relations.get("contact_id")
                )
                if relations:
                    logger.debug(f"🔗 Deal #{ext_id} → {relations}")
            
            count += 1
        
        logger.success(f"✅ {obj_type} : {count} synchronisés.")

    # 3. Rapport de Diff Automatique
    if snap_id > 1:
        logger.info(f"🔍 Comparaison avec le Snapshot précédent ({snap_id - 1})...")
        diff = DiffEngine(snap_id - 1, snap_id)
        report = diff.generate_report()
        
        logger.info(f"""
==================================================
📊 RAPPORT D'ACTIVITÉ - SNAPSHOT {snap_id}
✨ Nouveaux    : {len(report['created'])}
🔄 Modifiés    : {len(report['updated'])}
🗑️ Supprimés   : {len(report['deleted'])}
==================================================
        """)
        
        if report['updated']:
            changed_ids = [f"{item['type']}/{item['id']}" for item in report['updated']]
            logger.info(f"📝 Liste des changements : {changed_ids[:10]}")
    
    logger.success(f"🏁 Fin de session Zibridge (ID: {snap_id})")

if __name__ == "__main__":
    sync_all()