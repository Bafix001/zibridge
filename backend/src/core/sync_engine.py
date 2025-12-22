from loguru import logger
from sqlmodel import Session
from src.utils.db import engine
from src.connectors.factory import ConnectorFactory
from src.core.snapshot import SnapshotEngine
from src.core.models import Snapshot
from src.core.graph import GraphManager
from typing import Dict

def run_universal_sync(crm_type: str, credentials: dict):
    """
    🔥 VERSION AGNOSTIQUE TOTALE : détecte + sync TOUS les types automatiquement
    """
    snap_id = None
    success = False
    total_count = 0
    
    try:
        # 1. Instanciation dynamique via la Factory
        connector = ConnectorFactory.get_connector(crm_type, credentials)
        crm_name = type(connector).__name__.replace("Connector", "")
        
        # ✅ 2. AUTO-DÉTECTION DES TYPES
        entity_types = connector.get_detected_entities()
        detected_types = list(set(entity_types.values()))  # ['company', 'contact', 'custom_0']
        logger.info(f"🔍 DÉTECTÉ {len(detected_types)} types: {detected_types}")

        # ✅ 3. FIX source_name : récupère provider_name OU fallback
        source_name = credentials.get("provider_name", f"{crm_name}_File")

        # 4. Initialisation du Snapshot en base SQL (DYNAMIQUE + FIX)
        with Session(engine) as session:
            new_snap = Snapshot(
            source_name=source_name,
            source_type=connector.source_type,
            status="running",
            detected_entities={etype: connector.count_entities(etype) for etype in detected_types},
            companies_count=0,
            contacts_count=0,
            deals_count=0,
            tickets_count=0,
            project_id=credentials.get("project_id"),  # 🔥 LA LIGNE QUI MANQUAIT


            sync_config={
            "crm_type": crm_type,
            "provider_name": source_name,
            "source_type": connector.source_type,
            # On NE stocke PAS les credentials sensibles (tokens, passwords)
            # Seulement les métadonnées pour identifier la source
        }
        )
        
            logger.info(f"📌 Snapshot attaché au projet ID = {credentials.get('project_id')}")


            session.add(new_snap)
            session.commit()
            session.refresh(new_snap)
            snap_id = new_snap.id

        logger.info(f"🚀 DÉMARRAGE SYNC AGNOSTIQUE | Snap #{snap_id} | Source: {source_name} | Types: {detected_types}")

        # 5. Préparation des moteurs
        engine_snap = SnapshotEngine(snapshot_id=snap_id)
        graph_mgr = GraphManager()
        
        # ✅ 6. BOUCLE SUR TOUS LES TYPES DÉTECTÉS
        for entity_type in detected_types:
            count = 0
            logger.info(f"📥 Extraction {entity_type}s...")
            
            # Extraction des objets de CE TYPE
            for raw_item in connector.extract_entities(entity_type):
                ext_id = str(raw_item.get("id"))
                rels = raw_item.get("_zibridge_links", {}) 
                
                # Sauvegarde MinIO + Postgres
                engine_snap.process_item(
                    connector=connector,
                    object_type=f"{entity_type}s",  # companies, contacts, custom_0s
                    external_id=ext_id,
                    raw_data=raw_item,
                    associations=rels
                )
                
                # Suture Automatique du Graphe Neo4j
                for rel_type, target_ids in rels.items():
                    targets = target_ids if isinstance(target_ids, list) else [target_ids]
                    for t_id in targets:
                        graph_mgr.link_entities(
                            from_id=ext_id, from_type=f"{entity_type}s",
                            to_id=str(t_id), to_type=rel_type,
                            relation_name=f"REL_{entity_type}_TO_{rel_type}"
                        )
                
                count += 1
            
            # ✅ UPDATE COMPTEURS STANDARDS (mapping dynamique)
            if entity_type == "company":
                new_snap.companies_count = count
            elif entity_type == "contact":
                new_snap.contacts_count = count
            elif entity_type == "deal":
                new_snap.deals_count = count
            elif entity_type == "ticket":
                new_snap.tickets_count = count
            
            logger.success(f"✅ {entity_type}s synchronisés : {count}")
            total_count += count
        
        success = True

    except Exception as e:
        logger.error(f"❌ Erreur critique lors de la synchronisation universelle : {e}")
        success = False

    finally:
        # 7. Fermeture et mise à jour du statut final
        if snap_id:
            with Session(engine) as session:
                db_snap = session.get(Snapshot, snap_id)
                if db_snap:
                    db_snap.status = "completed" if success else "failed"
                    db_snap.total_objects = total_count
                    session.add(db_snap)
                    session.commit()
            
            status_label = "SUCCÈS" if success else "ÉCHEC"
            logger.info(f"🏁 FIN DE SYNCHRO : {status_label} ({total_count} items total)")

if __name__ == "__main__":
    pass
