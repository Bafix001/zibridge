from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, UploadFile, File, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from sqlmodel import Session, select, func
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import shutil
import os
import json
from datetime import datetime 

# Imports Zibridge
from src.core.models import SQLModel, Snapshot, SnapshotProject
from src.connectors.factory import ConnectorFactory
from src.utils.db import engine
from src.core.diff import DiffEngine
from src.core.restore import RestoreEngine
from src.core.sync_engine import run_universal_sync

app = FastAPI(
    title="Zibridge API",
    description="Universal Data Versioning System (Agnostic Engine)",
    version="1.3.0"
)

# 🔥 INITIALISATION
@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)
    logger.success("✅ Base prête")

# ✅ CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"]
)

UPLOAD_DIR = "data/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# --- SCHEMAS ---
class SyncRequest(BaseModel):
    crm_type: str 
    credentials: Dict[str, Any]
    project_id: Optional[int] = None  # ← AJOUTÉ

class RestoreRequest(BaseModel):
    selective: bool = True
    dry_run: bool = False
    properties: Optional[List[str]] = None
    crm_type: str = "hubspot"
    credentials: Dict[str, Any]

class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = None
    icon: str = "📸"
    default_source_type: str = "api"
    config: Dict[str, Any] = {}

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    default_source_type: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    display_order: Optional[int] = None

def get_session():
    with Session(engine) as session:
        yield session

# ========================================
# PROJETS (ONGLETS)
# ========================================

@app.get("/projects")
def list_projects(session: Session = Depends(get_session)):
    projects = session.exec(
        select(SnapshotProject).order_by(SnapshotProject.display_order, SnapshotProject.id)
    ).all()
    
    return [p.model_dump() for p in projects]

@app.get("/projects/{id}")
def get_project(id: int, session: Session = Depends(get_session)):
    project = session.get(SnapshotProject, id)
    if not project:
        raise HTTPException(status_code=404, detail="Projet introuvable")
    return project.model_dump()

@app.post("/projects")
def create_project(project_data: ProjectCreate, session: Session = Depends(get_session)):
    """Crée un nouveau projet (onglet)."""
    
    # 🔥 FIX : Ajouter source_type dans config
    config_with_type = {
        **project_data.config,
        "source_type": project_data.default_source_type,
        "provider": project_data.config.get("provider", "unknown")
    }
    
    project = SnapshotProject(
        name=project_data.name,
        description=project_data.description,
        icon=project_data.icon,
        default_source_type=project_data.default_source_type,
        config=config_with_type  
    )
    
    session.add(project)
    session.commit()
    session.refresh(project)
    
    logger.info(f"✅ Projet créé : {project.name} (type: {project.default_source_type})")
    return project.model_dump()


@app.patch("/projects/{id}")
def update_project(id: int, updates: ProjectUpdate, session: Session = Depends(get_session)):
    project = session.get(SnapshotProject, id)
    if not project:
        raise HTTPException(status_code=404, detail="Projet introuvable")
    
    for key, value in updates.model_dump(exclude_unset=True).items():
        setattr(project, key, value)
    
    project.updated_at = datetime.utcnow()
    session.add(project)
    session.commit()
    session.refresh(project)
    
    return project.model_dump()

@app.delete("/projects/{id}")
def delete_project(id: int, session: Session = Depends(get_session)):
    project = session.get(SnapshotProject, id)
    if not project:
        raise HTTPException(status_code=404, detail="Projet introuvable")
    
    session.delete(project)
    session.commit()
    
    return {"message": f"Projet {project.name} supprimé"}

@app.get("/projects/{id}/snapshots")
def get_project_snapshots(id: int, session: Session = Depends(get_session)):
    """Liste snapshots d'un projet."""
    snapshots = session.exec(
        select(Snapshot)
        .where(Snapshot.project_id == id)
        .order_by(Snapshot.id.desc())
    ).all()
    
    data_list = []
    for s in snapshots:
        item = s.model_dump()
        if item.get("created_at"):
            item["created_at"] = item["created_at"].isoformat()
        data_list.append(item)
    
    return data_list

# ========================================
# SNAPSHOTS
# ========================================

@app.get("/snapshots")
def list_snapshots(
    session: Session = Depends(get_session),
    project_id: Optional[int] = None
):
    statement = select(Snapshot)
    if project_id:
        statement = statement.where(Snapshot.project_id == project_id)
    
    snapshots = session.exec(statement.order_by(Snapshot.id.desc())).all()
    
    data_list = []
    for s in snapshots:
        item = s.model_dump()
        if item.get("created_at"):
            item["created_at"] = item["created_at"].isoformat()
        data_list.append(item)
    
    return data_list

@app.get("/snapshots/{id}")
def get_snapshot_detail(id: int, session: Session = Depends(get_session)):
    snapshot = session.get(Snapshot, id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot introuvable")
    
    data = snapshot.model_dump()
    if data.get("created_at"):
        data["created_at"] = data["created_at"].isoformat()
    
    return data

# ========================================
# SYNC (INGESTION)
# ========================================

@app.post("/api/sync/upload")
async def upload_csv_sync(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    provider: Optional[str] = Form(None),
    project_id: Optional[int] = Form(None)
):
    try:
        if not project_id:
            raise HTTPException(status_code=400, detail="project_id requis")
        
        # 🔥 NOUVEAU : Dossier par projet
        project_dir = os.path.join(UPLOAD_DIR, f"project_{project_id}")
        os.makedirs(project_dir, exist_ok=True)
        
        # Fichier source persistant
        source_file = os.path.join(project_dir, "source.csv")
        
        with open(source_file, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        logger.info(f"📥 Fichier {file.filename} → Projet {project_id} (stocké: {source_file})")
        
        # Sauvegarder le chemin dans le projet
        with Session(engine) as session:
            project = session.get(SnapshotProject, project_id)
            if project:
                project.config = {
                    "source_type": "file",
                    "file_path": source_file,
                    "original_filename": file.filename
                }
                session.add(project)
                session.commit()
        
        credentials = {
            "source_type": "file",
            "file_path": source_file, 
            "provider_name": "file",
            "project_id": project_id
        }
        
        background_tasks.add_task(run_universal_sync, "csv", credentials)
        
        return {"status": "upload_received", "filename": file.filename}
    except Exception as e:
        logger.error(f"Erreur upload : {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sync/start")
async def start_sync(request: SyncRequest, background_tasks: BackgroundTasks):
    logger.info(f"🚀 Sync {request.crm_type} → Projet {request.project_id}")
    
    credentials_with_project = {
        **request.credentials,
        "project_id": request.project_id,
        "provider_name": request.crm_type  # ← AJOUTÉ
    }
    
    background_tasks.add_task(run_universal_sync, request.crm_type, credentials_with_project)
    return {"status": "started", "source": request.crm_type}


@app.post("/projects/{project_id}/run")
async def run_project_sync(
    project_id: int,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session)
):
    """
    🔄 RE-SYNCHRONISE un projet existant.
    - Si fichier → Re-scanne le fichier stocké
    - Si API → Re-call l'API avec le token
    """
    project = session.get(SnapshotProject, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Projet introuvable")
    
    config = project.config or {}
    source_type = config.get("source_type")
    
    if source_type == "file":
        # Fichier : vérifier qu'il existe
        file_path = config.get("file_path")
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(
                status_code=400, 
                detail="Fichier source introuvable. Veuillez re-uploader."
            )
        
        credentials = {
            "source_type": "file",
            "file_path": file_path,
            "provider_name": "file",
            "project_id": project_id
        }
        
        background_tasks.add_task(run_universal_sync, "csv", credentials)
        logger.info(f"🔄 RUN projet {project_id} : Fichier {file_path}")
        
    elif source_type == "api":
        # API : besoin du token (on ne le stocke pas)
        raise HTTPException(
            status_code=400,
            detail="Pour les API, utilisez le bouton 'Nouvelle Capture' avec votre token"
        )
    
    else:
        raise HTTPException(status_code=400, detail="Type de source inconnu")
    
    return {
        "status": "started",
        "project_id": project_id,
        "message": "Re-synchronisation lancée"
    }

# ========================================
# DIFF & RESTORE
# ========================================

@app.get("/diff/{base}/{target}/details")
def get_diff(base: int, target: int):
    try:
        diff_engine = DiffEngine(base, target)
        return diff_engine.generate_detailed_report()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur Diff: {str(e)}")

@app.post("/api/restore/{snapshot_id}")
async def restore_snapshot(snapshot_id: int, config: RestoreRequest):
    try:
        connector = ConnectorFactory.get_connector(config.crm_type, config.credentials)
        restore_engine = RestoreEngine(snapshot_id=snapshot_id, connector=connector)
        report = restore_engine.run_smart_restore(selected_props=config.properties)
        return {"snapshot_id": snapshot_id, "report": report}
    except Exception as e:
        logger.error(f"Erreur Restore: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========================================
# STATS & HEALTH
# ========================================

@app.get("/stats")
def get_global_stats(session: Session = Depends(get_session)):
    total_snapshots = session.exec(select(func.count(Snapshot.id))).one()
    total_projects = session.exec(select(func.count(SnapshotProject.id))).one()
    return {
        "total_snapshots": total_snapshots,
        "total_projects": total_projects,
        "engine_status": "Agnostic v1.3"
    }

@app.get("/health")
def health():
    return {"status": "ready", "engine": "Zibridge-Agnostic v1.3"}

@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)