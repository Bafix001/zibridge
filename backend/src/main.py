import os
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel
from sqlmodel import Session, select, func

# Imports Zibridge
from src.core.models import SQLModel, Snapshot, SnapshotProject, Branch, NormalizedStorage
from src.utils.db import engine
from src.core.diff import DiffEngine
from src.core.restore import RestoreEngine
from src.core.sync_engine import run_universal_sync
from src.core.snapshot import SnapshotEngine

app = FastAPI(
    title="Zibridge API",
    description="Git-like Data Versioning System for CRM",
    version="1.4.0"
)

# ✅ CORS & REFINE HEADERS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"]
)

@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)
    logger.success("🚀 Zibridge Backend: Ready")

def get_session():
    with Session(engine) as session:
        yield session

# --- SCHEMAS ---
class BranchCreate(BaseModel):
    name: str
    project_id: int
    base_snapshot_id: Optional[int] = None

# ========================================
# 1. PROJETS & BRANCHES
# ========================================

@app.get("/projects")
def list_projects(response: Response, session: Session = Depends(get_session)):
    statement = select(SnapshotProject).order_by(SnapshotProject.display_order)
    projects = session.exec(statement).all()
    response.headers["X-Total-Count"] = str(len(projects))
    return projects

@app.get("/branches")
def list_branches(response: Response, project_id: Optional[int] = None, session: Session = Depends(get_session)):
    statement = select(Branch)
    if project_id:
        statement = statement.where(Branch.project_id == project_id)
    branches = session.exec(statement).all()
    response.headers["X-Total-Count"] = str(len(branches))
    return branches

# ========================================
# 2. SNAPSHOTS (COMMITS)
# ========================================

@app.get("/snapshots")
def list_snapshots(
    response: Response, 
    project_id: Optional[int] = None, 
    branch_id: Optional[int] = None, 
    session: Session = Depends(get_session)
):
    """Liste les snapshots avec support des filtres Refine."""
    statement = select(Snapshot).order_by(Snapshot.created_at.desc())
    if project_id:
        statement = statement.where(Snapshot.project_id == project_id)
    if branch_id:
        statement = statement.where(Snapshot.branch_id == branch_id)
    
    snapshots = session.exec(statement).all()
    response.headers["X-Total-Count"] = str(len(snapshots))
    return snapshots

# ========================================
# 3. ENGINE OPERATIONS
# ========================================

@app.post("/sync/start")
async def start_sync(
    crm_type: str = Form(...),
    project_id: int = Form(...),
    branch_id: int = Form(...),
    token: Optional[str] = Form(None),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Lance un 'Commit' (Capture de l'état du CRM)."""
    credentials = {"project_id": project_id, "token": token}
    background_tasks.add_task(run_universal_sync, crm_type, credentials, branch_id=branch_id)
    return {"status": "started", "branch_id": branch_id}

@app.get("/diff/{base_id}/{target_id}")
def get_diff(base_id: int, target_id: int, project_id: int):
    """Calcule le diff PR Style entre deux états."""
    diff_engine = DiffEngine(base_id, target_id, project_id=project_id)
    return diff_engine.generate_detailed_report()

@app.post("/restore/{snapshot_id}")
async def restore_snapshot(
    snapshot_id: int, 
    project_id: int, 
    crm_type: str, 
    token: str,
    background_tasks: BackgroundTasks
):
    """Déclenche la suture chirurgicale vers le CRM."""
    from src.connectors.factory import ConnectorFactory
    
    # On instancie le connecteur à la volée
    connector = ConnectorFactory.get_connector(crm_type, {"token": token, "project_id": project_id})
    restore_engine = RestoreEngine(project_id, connector, snapshot_id=snapshot_id)
    
    # Lancement en arrière-plan
    background_tasks.add_task(restore_engine.run)
    return {"status": "restoration_started"}

@app.get("/health")
def health():
    return {"status": "ready", "mode": "elon-musk-agnostic"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)