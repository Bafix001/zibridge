import json
import io
from contextlib import contextmanager
from typing import Generator, Dict, Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session 
from neo4j import GraphDatabase
from minio import Minio
from loguru import logger

from src.utils.config import settings

# --- POSTGRES (Relationnel & Métadonnées) ---
postgres_url = f"postgresql://{settings.postgres.user}:{settings.postgres.password}@{settings.postgres.host}:{settings.postgres.port}/{settings.postgres.db}"
# On ajoute pool_pre_ping pour éviter les "stale connections" sur les longs processus de sync
engine = create_engine(postgres_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db_session() -> Generator[Session, None, None]:  
    session = SessionLocal()
    try:
        yield session
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Erreur DB SQL: {e}")
        raise 
    finally:
        session.close()    

# --- NEO4J (Graphe de Suture) ---
neo4j_driver = GraphDatabase.driver(
    settings.neo4j.uri, 
    auth=(settings.neo4j.user, settings.neo4j.password)
)

# --- MINIO (Storage Manager - Blobs JSON) ---
class StorageManager:
    def __init__(self):
        # Nettoyage de l'endpoint
        endpoint = settings.minio.endpoint.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint,
            access_key=settings.minio.root_user,
            secret_key=settings.minio.root_password,
            secure=False
        )
        self.bucket = settings.minio.bucket
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Vérifie ou crée le bucket au démarrage."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"🪣 Bucket MinIO '{self.bucket}' créé avec succès.")
        except Exception as e:
            logger.error(f"❌ Impossible de vérifier/créer le bucket MinIO: {e}")

    def save_json(self, path: str, data: Dict[str, Any]):
        """Sauvegarde un dictionnaire en JSON de manière atomique."""
        try:
            content = json.dumps(data, default=str).encode('utf-8')
            self.client.put_object(
                self.bucket, path,
                data=io.BytesIO(content),
                length=len(content),
                content_type='application/json'
            )
        except Exception as e:
            logger.error(f"❌ Erreur sauvegarde MinIO ({path}): {e}")
            raise

    def get_json(self, path: str) -> Dict[str, Any]:
        """Récupère un fichier JSON avec gestion propre de la connexion."""
        try:
            response = self.client.get_object(self.bucket, path)
            return json.loads(response.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"❌ Erreur lecture MinIO ({path}): {e}")
            raise
        finally:
            if 'response' in locals():
                response.close()
                response.release_conn()

# Singleton pour l'application
storage_manager = StorageManager()