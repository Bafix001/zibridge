import json
import io
from contextlib import contextmanager
from typing import Generator, Dict, Any

from sqlalchemy import QueuePool, create_engine
from sqlalchemy.orm import sessionmaker, Session 
from sqlalchemy.pool import QueuePool  # ✅ CHANGÉ
from neo4j import GraphDatabase
from minio import Minio
from loguru import logger

from src.utils.config import settings

# --- POSTGRES (FIXÉ) ---
postgres_url = f"postgresql://{settings.postgres.user}:{settings.postgres.password}@{settings.postgres.host}:{settings.postgres.port}/{settings.postgres.db}"

engine = create_engine(
    postgres_url,
    pool_size=20,          # Permet 20 connexions simultanées pour Refine
    max_overflow=10,       # Peut monter à 30 en cas de gros Snapshots
    pool_recycle=3600,
    connect_args={"connect_timeout": 5},
    echo=False
)

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

# --- NEO4J ---
try:
    neo4j_driver = GraphDatabase.driver(
        settings.neo4j.uri, 
        auth=(settings.neo4j.user, settings.neo4j.password)
    )
    logger.success("✅ Neo4j driver créé")
except Exception as e:
    logger.warning(f"⚠️ Neo4j non disponible: {e}")
    neo4j_driver = None

# --- MINIO (IDENTIQUE) ---
class StorageManager:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        endpoint = settings.minio.endpoint.replace("http://", "").replace("https://", "")
        try:
            self.client = Minio(
                endpoint,
                access_key=settings.minio.root_user,
                secret_key=settings.minio.root_password,
                secure=False
            )
            self.bucket = settings.minio.bucket
            self._ensure_bucket_exists()
            self._initialized = True
            logger.success("✅ MinIO initialisé")
        except Exception as e:
            logger.warning(f"⚠️ MinIO non disponible: {e}")
            self._initialized = False
    
    def _ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"🪣 Bucket '{self.bucket}' créé")
        except Exception as e:
            logger.error(f"❌ Bucket MinIO: {e}")
            raise
    
    def save_json(self, path: str, data: Dict[str, Any]):
        if not self._initialized:
            raise RuntimeError("StorageManager non initialisé")
        try:
            content = json.dumps(data, default=str).encode('utf-8')
            self.client.put_object(self.bucket, path, data=io.BytesIO(content), length=len(content), content_type='application/json')
        except Exception as e:
            logger.error(f"❌ MinIO save {path}: {e}")
            raise
    
    def get_json(self, path: str) -> Dict[str, Any]:
        if not self._initialized:
            raise RuntimeError("StorageManager non initialisé")
        try:
            response = self.client.get_object(self.bucket, path)
            return json.loads(response.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"❌ MinIO get {path}: {e}")
            raise
        finally:
            if 'response' in locals():
                response.close()
                response.release_conn()

# On crée l'instance immédiatement au chargement du module
storage_manager = StorageManager()

def get_storage_manager() -> StorageManager:
    """Retourne l'instance unique du gestionnaire de stockage."""
    return storage_manager