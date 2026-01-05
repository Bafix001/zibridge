import os
from sqlmodel import SQLModel, Session
from src.utils.db import engine
from src.core.models import (
    Snapshot, 
    SnapshotItem, 
    SnapshotProject, 
    Branch, 
    NormalizedStorage,
    IdMapping,
    Blob
)
from loguru import logger

def create_db_and_tables(force: bool = False):
    """
    🔨 Initialisation de la base PostgreSQL Zibridge.
    """
    if force:
        logger.warning("🗑️  MODE FORCE : Suppression des données existantes...")
        SQLModel.metadata.drop_all(engine)
    
    logger.info("🔨 Création des tables SQLModel...")
    SQLModel.metadata.create_all(engine)
    
    # --- SEED DATA (Optionnel) ---
    # On crée un projet par défaut si la table est vide
    with Session(engine) as session:
        from sqlmodel import select
        existing_project = session.exec(select(SnapshotProject)).first()
        if not existing_project:
            logger.info("🌱 Création du projet de démonstration...")
            demo_project = SnapshotProject(
                name="Projet Démo CRM",
                description="Migration HubSpot vers Salesforce",
                config={"mappings": {"contact": {"unique_id": "email"}}}
            )
            session.add(demo_project)
            session.commit()
            session.refresh(demo_project)
            
            # Création de la branche main
            main_branch = Branch(name="main", project_id=demo_project.id)
            session.add(main_branch)
            session.commit()

    logger.success("✅ Base de données Zibridge prête !")

if __name__ == "__main__":
    # Sécurité : on ne drop que si on le demande explicitement via variable d'env
    is_dev = os.getenv("ZIBRIDGE_ENV") == "development"
    create_db_and_tables(force=is_dev)