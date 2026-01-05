from loguru import logger
from sqlmodel import Session, select
from src.utils.db import engine
from src.connectors.factory import ConnectorFactory
from src.core.snapshot import SnapshotEngine
from src.core.models import Snapshot, Branch
from src.core.graph import GraphManager
from typing import Dict, List, Optional

def run_universal_sync(crm_type: str, credentials: dict, branch_id: Optional[int] = None):
    """
    🚀 MOTEUR DE RÉSILIENCE ZIBRIDGE (Orchestrateur).
    Traite le CRM comme un repo Git : Discovery -> Extraction -> Hashing -> Graph Mapping.
    """
    snap_id = None
    success = False
    total_count = 0
    BATCH_SIZE = 500 
    project_id = credentials.get("project_id")
    final_stats = {}
    
    try:
        # 1. Initialisation du connecteur agnostique
        connector = ConnectorFactory.get_connector(crm_type, credentials)
        source_name = credentials.get("provider_name", crm_type)
        
        # 2. DÉCOUVERTE DYNAMIQUE (Elon Mode)
        # On ne liste plus 'contacts/companies' en dur, on demande au CRM ce qu'il a.
        detected_types = connector.get_available_object_types()
        logger.info(f"🛰️ Types détectés pour {source_name}: {detected_types}")

        # 3. CRÉATION DU COMMIT (Snapshot)
        snap_id = create_snapshot(
            source_name=source_name,
            source_type=connector.source_type,
            project_id=project_id,
            branch_id=branch_id,
            crm_type=crm_type
        )

        # 4. INITIALISATION DES MOTEURS
        snapshot_engine = SnapshotEngine(snapshot_id=snap_id)
        
        # 5. EXTRACTION & VERSIONNING (Streaming)
        for entity_type in detected_types:
            count = sync_entity_type(
                connector=connector,
                entity_type=entity_type,
                snapshot_engine=snapshot_engine,
                batch_size=BATCH_SIZE
            )
            final_stats[entity_type] = count
            total_count += count
        
        # 6. FINALISATION GÉOMÉTRIQUE
        # Calcule le Merkle Root global et ferme le snapshot
        snapshot_engine.finalize_snapshot()
        success = True

    except Exception as e:
        logger.error(f"❌ Erreur critique lors de la synchro: {e}", exc_info=True)
        success = False

    finally:
        if snap_id:
            finalize_db_record(snap_id, branch_id, success, total_count, final_stats)
    
    return snap_id, success, total_count


def create_snapshot(source_name: str, source_type: str, project_id: int, 
                   branch_id: Optional[int], crm_type: str) -> int:
    """Initialise le snapshot dans PostgreSQL."""
    with Session(engine) as session:
        snapshot = Snapshot(
            source_name=source_name,
            source_type=source_type,
            status="running",
            project_id=project_id,
            branch_id=branch_id,
            sync_config={"crm_type": crm_type}
        )
        session.add(snapshot)
        session.commit()
        session.refresh(snapshot)
        return snapshot.id


def sync_entity_type(connector, entity_type: str, snapshot_engine, batch_size: int) -> int:
    """Aspire un type d'objet et ses relations en Single-Pass."""
    count = 0
    
    # On utilise extract_entities qui normalise déjà les données et IDs
    for item in connector.extract_entities(entity_type):
        entity_id = str(item.get("id"))
        links = item.get("_zibridge_links", [])
        
        # SnapshotEngine s'occupe de : Hashing + Blob Storage + Graph Mapping
        snapshot_engine.process_item(
            connector=connector,
            object_type=entity_type,
            external_id=entity_id,
            raw_data=item,
            associations=links
        )
        count += 1
        
        # Le flush interne du SnapshotEngine gère les paquets de 500
    
    logger.info(f"✅ Terminé : {count} {entity_type} versionnés.")
    return count


def finalize_db_record(snap_id: int, branch_id: Optional[int], success: bool, total: int, stats: Dict[str, int]):
    """Met à jour les métadonnées finales et le pointeur HEAD de la branche."""
    with Session(engine) as session:
        snapshot = session.get(Snapshot, snap_id)
        if snapshot:
            snapshot.status = "completed" if success else "failed"
            snapshot.total_objects = total
            snapshot.stats = stats # Stockage JSON agnostique des compteurs
            
            session.add(snapshot)
            
            # 🔥 GIT LOGIC: HEAD Update
            if success and branch_id:
                branch = session.get(Branch, branch_id)
                if branch:
                    branch.current_snapshot_id = snap_id
                    session.add(branch)
            
            session.commit()