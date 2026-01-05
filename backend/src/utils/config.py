from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class PostgresSettings(BaseSettings):
    # On garde les alias car ton .env utilise les majuscules
    user: str = Field(alias="POSTGRES_USER")
    password: str = Field(alias="POSTGRES_PASSWORD")
    db: str = Field(alias="POSTGRES_DB")
    host: str = Field(alias="POSTGRES_HOST")
    port: int = Field(alias="POSTGRES_PORT")
    
    # On dit à chaque sous-classe de lire aussi le .env
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

class Neo4jSettings(BaseSettings):
    uri: str = Field(alias="NEO4J_URI")
    user: str = Field(alias="NEO4J_USER")
    password: str = Field(alias="NEO4J_PASSWORD")
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

class MinioSettings(BaseSettings):
    endpoint: str = Field(alias="MINIO_ENDPOINT")
    root_user: str = Field(alias="MINIO_ROOT_USER")
    root_password: str = Field(alias="MINIO_ROOT_PASSWORD")
    bucket: str = Field(alias="MINIO_BUCKET")
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

class Settings(BaseSettings):
# Ajout d'un tag d'environnement pour ton "Storage de Normalisation"  
    app_env: str = Field(default="development", alias="APP_ENV")
    # Ici, on instancie les classes. 
    # Elles iront chercher leurs propres variables grâce à leur model_config
    postgres: PostgresSettings = PostgresSettings()
    neo4j: Neo4jSettings = Neo4jSettings()
    minio: MinioSettings = MinioSettings()
    
    # HubSpot Token (directement dans la classe parente pour simplifier)
    hubspot_access_token: Optional[str] = Field(default=None, alias="HUBSPOT_ACCESS_TOKEN")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

# Instance globale
settings = Settings()