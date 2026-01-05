import warnings
import os
import sys
import argparse
from loguru import logger
from sqlmodel import Session
from datetime import datetime, timezone
from dotenv import load_dotenv

# Initialisation des chemins
load_dotenv(os.path.join(os.getcwd(), ".env"))
sys.path.append(os.getcwd())

from src.utils.db import engine
from src.connectors.factory import ConnectorFactory
from src.core.snapshot import SnapshotEngine
from src.core.models import Snapshot, SnapshotProject, Branch
from src.core.graph import GraphManager

# Clean warnings
warnings.filterwarnings("ignore", message=".*OpenSSL 1.1.1+.*")

def run_sync(project_id: int, branch_id: int):
    """
    🚀 ORCHESTRATEUR DE COMMITS ZIBRIDGE.
    """
    with Session(engine) as session:
        project = session.get(SnapshotProject, project_id)
        if not project:
            logger.error(f"❌ Projet {project_id} introuvable.")
            return

        # 1. Initialisation Connecteur
        try:
            connector = ConnectorFactory.get_connector(
                project.default_source_type,
                project.config,
                project_id=project_id,
                project_config=project.config
            )

        except Exception as e:
            logger.error(f"❌ Initialisation connecteur échouée: {e}")
            return

        # 2. Création du Snapshot (Commit)
        new_snap = Snapshot(
            project_id=project_id,
            branch_id=branch_id,
            source_name=f"{project.default_source_type.upper()}",
            source_type="api",
            status="running",
            created_at=datetime.now(timezone.utc)
        )
        session.add(new_snap)
        session.commit()
        session.refresh(new_snap)
    
        logger.info(f"🛰️ START SNAPSHOT #{new_snap.id} | PROJECT: {project.name}")
        
        success = False
        total_count = 0
        final_stats = {}
        graph_mgr = GraphManager()
        engine_snap = SnapshotEngine(snapshot_id=new_snap.id)
        
        # Reset de la topologie locale pour ce projet
        graph_mgr.clear_project_graph(project_id)

        try:
            # 3. Découverte Agnostique : Config ou Auto-détection
            objects = project.config.get("sync_objects")
            if not objects:
                logger.info("🔍 Aucune liste d'objets définie. Auto-détection en cours...")
                objects = connector.get_available_object_types()
            
            for obj_type in objects:
                count = 0
                links_batch = []
                logger.info(f"📥 Extraction: {obj_type}")
                
                # Streaming extraction (Elon Mode)
                for item in connector.extract_entities(obj_type):
                    ext_id = str(item.get("id"))
                    rels = item.get("_zibridge_links", {})
                    
                    # Versionnage immuable
                    engine_snap.process_item(
                        connector=connector,
                        object_type=obj_type,
                        external_id=ext_id,
                        raw_data=item,
                        associations=rels
                    )
                    
                    if rels:
                        # On supporte les formats HubSpot et agnostiques
                        for target_type, target_ids in rels.items():
                            t_list = target_ids if isinstance(target_ids, list) else [target_ids]
                            for t_id in t_list:
                                links_batch.append({
                                    "from_id": ext_id,
                                    "to_id": str(t_id),
                                    "to_type": target_type
                                })
                    count += 1
                
                if links_batch:
                    graph_mgr.link_entities_batch(project_id, obj_type, links_batch)

                final_stats[obj_type] = count
                total_count += count
            
            # 4. Finalisation technique (Merkle Roots, Flush SQL)
            engine_snap.finalize_snapshot() 
            success = True

        except Exception as e:
            logger.error(f"❌ Erreur critique : {e}")
            success = False
            
        finally:
            # On rouvre une session propre pour la mise à jour finale
            with Session(engine) as session:
                snap_to_update = session.get(Snapshot, new_snap.id)
                if snap_to_update:
                    final_status = "completed" if success else "failed"
                    snap_to_update.status = final_status
                    snap_to_update.total_objects = total_count
                    snap_to_update.stats = final_stats
                    
                    if success:
                        branch = session.get(Branch, branch_id)
                        if branch:
                            branch.current_snapshot_id = snap_to_update.id
                            session.add(branch)
                            
                    session.add(snap_to_update)
                    session.commit()
                    # On loggue AVANT que la session ne se ferme ou via une variable locale
                    logger.info(f"🏁 FIN: {final_status.upper()} ({total_count} items)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=int, required=True)
    parser.add_argument("--branch", type=int, required=True)
    args = parser.parse_args()
    
    run_sync(args.project, args.branch)