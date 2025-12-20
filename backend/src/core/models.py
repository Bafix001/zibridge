from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel, create_engine

class Snapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str  # ex: "hubspot"
    status: str = "pending" # pending, completed, failed

class Blob(SQLModel, table=True):
    """L'archive unique. Identifiée par son hash SHA-256."""
    hash: str = Field(primary_key=True)
    content_type: str  # ex: "contact"
    created_at: datetime = Field(default_factory=datetime.utcnow)

class SnapshotItem(SQLModel, table=True):
    """Le lien entre un snapshot et un objet à un instant T."""
    id: Optional[int] = Field(default=None, primary_key=True)
    snapshot_id: int = Field(foreign_key="snapshot.id")
    object_id: str  # L'ID original (ex: ID HubSpot "101")
    object_type: str # ex: "contact"
    content_hash: str = Field(foreign_key="blob.hash")

# 🆕 NOUVEAU MODÈLE : Tracking des changements d'ID
class IdMapping(SQLModel, table=True):
    """
    Stocke les mappings old_id → new_id quand un objet est restauré avec un nouvel ID.
    Essentiel pour recréer les associations après une restauration.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    snapshot_id: int  # Snapshot source de la restauration
    object_type: str  # "contacts", "companies", "deals"
    old_id: str  # ID dans le snapshot (ancien ID HubSpot)
    new_id: str  # Nouvel ID HubSpot après restauration
    created_at: datetime = Field(default_factory=datetime.utcnow)