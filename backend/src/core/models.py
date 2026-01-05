from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlmodel import Field, SQLModel, JSON, Column

# --- PROJET ---
class SnapshotProject(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: Optional[str] = None
    icon: str = Field(default="📸")
    default_source_type: str = Field(default="api")
    # Stocke le mapping agnostique et les objets activés
    enabled_objects: List[str] = Field(default_factory=list, sa_column=Column(JSON))
    config: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    display_order: int = Field(default=0)

# --- GIT : BRANCHES ---
class Branch(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="snapshotproject.id", index=True)
    name: str = Field(index=True)
    base_snapshot_id: Optional[int] = None
    current_snapshot_id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

# --- GIT : COMMITS (SNAPSHOTS) ---
class Snapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="snapshotproject.id", index=True)
    branch_id: Optional[int] = Field(foreign_key="branch.id", index=True, nullable=True)
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    source_name: str 
    source_type: str = Field(default="api")
    status: str = Field(default="pending", index=True) 
    
    # Stats dynamiques (Elon Mode)
    stats: Dict[str, int] = Field(default_factory=dict, sa_column=Column(JSON))
    total_objects: int = Field(default=0)
    
    # Merkle Root pour le Smart Diff
    root_hash: Optional[str] = Field(default=None, index=True)
    
    detected_entities: Dict[str, int] = Field(default_factory=dict, sa_column=Column(JSON))
    snapshot_metadata: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))
    sync_config: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))

# --- STORAGE : BLOBS (IMMUABLE) ---
class Blob(SQLModel, table=True):
    hash: str = Field(primary_key=True, max_length=64)
    content_type: str = Field(index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

# --- LIEN ITEM <-> VERSION ---
class SnapshotItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    snapshot_id: int = Field(foreign_key="snapshot.id", index=True)
    object_id: str = Field(index=True) 
    object_type: str = Field(index=True) 
    content_hash: str = Field(foreign_key="blob.hash", index=True)
    source_namespace: str = Field(default="default", index=True)

# --- STORAGE FINAL (NORMALISÉ) ---
# 🛡️ C'est cette classe qui manquait dans ton erreur !
class NormalizedStorage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="snapshotproject.id", index=True)
    object_type: str = Field(index=True) 
    global_id: str = Field(index=True) 
    data: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    
    last_snapshot_id: int = Field(index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)

# --- MAPPING D'IDS ---
class IdMapping(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="snapshotproject.id", index=True)
    object_type: str = Field(index=True)
    source_system: str = Field(index=True)
    old_id: str = Field(index=True)
    new_id: str = Field(index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)