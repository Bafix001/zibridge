import json
import io
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session 
from neo4j import GraphDatabase
from minio import Minio
from loguru import logger

from src.utils.config import settings

# --- POSTGRES ---
postgres_url = f"postgresql://{settings.postgres.user}:{settings.postgres.password}@{settings.postgres.host}:{settings.postgres.port}/{settings.postgres.db}"
engine = create_engine(postgres_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db_session() -> Generator[Session, None, None]:  
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise 
    finally:
        session.close()    

# --- NEO4J ---
neo4j_driver = GraphDatabase.driver(
    settings.neo4j.uri, 
    auth=(settings.neo4j.user, settings.neo4j.password)
)

@contextmanager
def get_neo4j_session():
    session = neo4j_driver.session()
    try:
        yield session
    except Exception as e:
        logger.error(f"Erreur Neo4j: {e}")
        raise
    finally:
        session.close()

# --- MINIO (Storage Manager) ---
class StorageManager:
    def __init__(self):
        # On nettoie l'endpoint (on enlève http:// car la lib Minio le gère via 'secure')
        endpoint = settings.minio.endpoint.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint,
            access_key=settings.minio.root_user,
            secret_key=settings.minio.root_password,
            secure=False
        )
        self.bucket = settings.minio.bucket

    def save_json(self, path: str, data: dict):
        """Sauvegarde un dictionnaire en JSON dans MinIO."""
        content = json.dumps(data).encode('utf-8')
        self.client.put_object(
            self.bucket, path,
            data=io.BytesIO(content),
            length=len(content),
            content_type='application/json'
        )

    def get_json(self, path: str) -> dict:
        """Récupère un fichier JSON depuis MinIO."""
        response = self.client.get_object(self.bucket, path)
        try:
            return json.loads(response.read().decode('utf-8'))
        finally:
            response.close()
            response.release_conn()

# On instancie l'objet unique
storage_manager = StorageManager()