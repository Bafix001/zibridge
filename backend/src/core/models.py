from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlmodel import Field, SQLModel, JSON, Column


class SnapshotProject(SQLModel, table=True):
    """
    Onglet/Workspace isolé pour regrouper des snapshots.
    Exemple : "Import CSV Q4", "HubSpot Production", "Test Salesforce"
    100% AGNOSTIQUE - s'adapte à tout type de source.
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)  # "Import CSV Q4"
    description: Optional[str] = None  # Description optionnelle
    icon: str = Field(default="📸")  # Emoji/icône personnalisé
    
    # Type de source par défaut pour ce projet
    default_source_type: str = Field(default="api")  # "api" ou "file"
    
    # Configuration agnostique (JSON flexible)
    config: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    # Exemples :
    # {"provider": "hubspot", "token": "xxx"}
    # {"provider": "salesforce", "instance_url": "xxx", "token": "yyy"}
    # {"provider": "csv", "default_path": "/uploads/"}
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Ordre d'affichage dans la sidebar
    display_order: int = Field(default=0)


class Snapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # 🔥 Lien vers le projet parent (onglet)
    project_id: int = Field(
    foreign_key="snapshotproject.id",
    index=True,
    nullable=False
)

    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    source_name: str  # ex: "CSV_File", "HubSpot_Prod"
    source_type: str = Field(default="api")  # api, file, database
    status: str = Field(default="pending") 
    
    # ✅ COMPTEURS DYNAMIQUES (backward compatible)
    companies_count: int = Field(default=0)
    contacts_count: int = Field(default=0)
    deals_count: int = Field(default=0)
    tickets_count: int = Field(default=0)
    total_objects: int = Field(default=0)
    
    # 🔥 AGNOSTIQUE TOTAL
    detected_entities: Dict[str, int] = Field(default_factory=dict, sa_column=Column(JSON))
    # Ex: {"company": 100, "contact": 100, "custom_0": 50}
    
    snapshot_metadata: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))


class Blob(SQLModel, table=True):
    """L'archive unique (CAS). Identifiée par son hash SHA-256."""
    hash: str = Field(primary_key=True)
    content_type: str  # ex: "company", "contact", "custom_0"
    created_at: datetime = Field(default_factory=datetime.utcnow)


class SnapshotItem(SQLModel, table=True):
    """Le lien entre un snapshot et un objet à un instant T."""
    id: Optional[int] = Field(default=None, primary_key=True)
    snapshot_id: int = Field(foreign_key="snapshot.id", index=True)
    object_id: str  # L'ID original de la source
    object_type: str  # AGNOSTIQUE: "companies", "custom_0s"
    content_hash: str = Field(foreign_key="blob.hash", index=True)
    source_namespace: str = Field(default="default", index=True)


class IdMapping(SQLModel, table=True):
    """Table de suture : réconcilie les IDs entre le backup et le CRM actuel."""
    id: Optional[int] = Field(default=None, primary_key=True)
    snapshot_id: int = Field(index=True)
    object_type: str  # AGNOSTIQUE: "companies", "custom_0s"
    old_id: str 
    new_id: str 
    created_at: datetime = Field(default_factory=datetime.utcnow)


class SQLModelBase(SQLModel):
    """Classe de base pour tous les objets versionnés."""
    id: Optional[int] = Field(default=None, primary_key=True)
    external_id: str
    snapshot_id: int
    object_type: str  # "companies", "contacts", "custom_0s"
    created_at: datetime = Field(default_factory=datetime.utcnow)