from sqlmodel import SQLModel
from src.utils.db import engine
# IMPORTANT : Importer les modèles pour que SQLModel les connaisse
from src.core.models import Snapshot, Blob, SnapshotItem, IdMapping

def create_db_and_tables():
    print("🔨 Création des tables dans PostgreSQL...")
    SQLModel.metadata.create_all(engine)
    print("✅ Tables créées avec succès !")

if __name__ == "__main__":
    create_db_and_tables()