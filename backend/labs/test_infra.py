from loguru import logger
from src.utils.config import settings
import psycopg2
from minio import Minio

def check_infra():
    # Test Postgres
    try:
        conn = psycopg2.connect(
            host=settings.postgres.host,
            user=settings.postgres.user,
            password=settings.postgres.password,
            dbname=settings.postgres.db,
            port=settings.postgres.port
        )
        logger.success("PostgreSQL: Connexion réussie !")
        conn.close()
    except Exception as e:
        logger.error(f"PostgreSQL: Échec -> {e}")

    # Test MinIO
    try:
        client = Minio(
            settings.minio.endpoint,
            access_key=settings.minio.root_user,
            secret_key=settings.minio.root_password,
            secure=False # Car on est en local
        )
        logger.success("MinIO: Connexion réussie !")
    except Exception as e:
        logger.error(f"MinIO: Échec -> {e}")

if __name__ == "__main__":
    check_infra()