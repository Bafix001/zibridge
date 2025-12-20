from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select, func
from typing import List, Optional
from datetime import datetime

from src.utils.db import engine
from src.core.models import Snapshot, SnapshotItem
from src.core.diff import DiffEngine
from src.core.restore import RestoreEngine
from src.utils.db import storage_manager

app = FastAPI(
    title="Zibridge API",
    description="Git for Data - CRM Versioning System",
    version="1.0.0"
)

# CORS pour le frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_session():
    with Session(engine) as session:
        yield session

# ========================================
# ENDPOINTS SNAPSHOTS
# ========================================

@app.get("/snapshots")
def list_snapshots(
    _start: int = 0,
    _end: int = 10,
    _sort: str = "id",
    _order: str = "DESC",
    session: Session = Depends(get_session)
):
    """Liste des snapshots avec pagination (format Refine)"""
    
    # Requête de base
    statement = select(Snapshot)
    
    # Tri
    if _order.upper() == "DESC":
        statement = statement.order_by(Snapshot.id.desc())
    else:
        statement = statement.order_by(Snapshot.id.asc())
    
    # Pagination
    total = session.exec(select(func.count()).select_from(Snapshot)).one()
    snapshots = session.exec(statement.offset(_start).limit(_end - _start)).all()
    
    # Enrichir avec le nombre d'items
    result = []
    for snap in snapshots:
        count_stmt = select(func.count()).select_from(SnapshotItem).where(
            SnapshotItem.snapshot_id == snap.id
        )
        item_count = session.exec(count_stmt).one()
        
        result.append({
            "id": snap.id,
            "source": snap.source,
            "status": snap.status,
            "timestamp": snap.timestamp.isoformat() if hasattr(snap.timestamp, 'isoformat') else str(snap.timestamp),
            "item_count": item_count
        })
    
    return result

@app.get("/snapshots/{id}")
def get_snapshot(id: int, session: Session = Depends(get_session)):
    """Détails d'un snapshot"""
    
    snapshot = session.get(Snapshot, id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    
    # Compter les items
    count_stmt = select(func.count()).select_from(SnapshotItem).where(
        SnapshotItem.snapshot_id == id
    )
    item_count = session.exec(count_stmt).one()
    
    # Récupérer les items par type
    items_stmt = select(SnapshotItem).where(SnapshotItem.snapshot_id == id)
    items = session.exec(items_stmt).all()
    
    items_by_type = {}
    for item in items:
        if item.object_type not in items_by_type:
            items_by_type[item.object_type] = 0
        items_by_type[item.object_type] += 1
    
    return {
        "id": snapshot.id,
        "source": snapshot.source,
        "status": snapshot.status,
        "timestamp": snapshot.timestamp.isoformat() if hasattr(snapshot.timestamp, 'isoformat') else str(snapshot.timestamp),
        "item_count": item_count,
        "items_by_type": items_by_type
    }

# ========================================
# ENDPOINTS DIFF
# ========================================

@app.get("/diff/{base}/{target}")
def compare_snapshots(base: int, target: int):
    """Compare deux snapshots"""
    
    try:
        diff_engine = DiffEngine(base, target)
        report = diff_engine.generate_report()
        
        return {
            "base": base,
            "target": target,
            "summary": {
                "created": len(report["created"]),
                "updated": len(report["updated"]),
                "deleted": len(report["deleted"])
            },
            "details": {
                "created": report["created"],
                "updated": report["updated"],
                "deleted": report["deleted"]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/diff/{base}/{target}/details")
def compare_snapshots_details(base: int, target: int):
    """Compare deux snapshots avec détails des modifications"""
    
    try:
        diff_engine = DiffEngine(base, target)
        report = diff_engine.generate_report()
        
        # Enrichir les updates avec les détails
        detailed_updates = []
        for item in report["updated"]:
            # Récupérer les JSONs
            old_json = storage_manager.get_json(f"blobs/{item['old_hash']}.json")
            new_json = storage_manager.get_json(f"blobs/{item['new_hash']}.json")
            
            p1 = old_json.get('properties', old_json)
            p2 = new_json.get('properties', new_json)
            
            # Calculer les changements
            changes = {}
            for key in set(p1.keys()) | set(p2.keys()):
                val1, val2 = p1.get(key), p2.get(key)
                if val1 != val2:
                    changes[key] = {"old": val1, "new": val2}
            
            detailed_updates.append({
                **item,
                "changes": changes
            })
        
        return {
            "base": base,
            "target": target,
            "summary": {
                "created": len(report["created"]),
                "updated": len(report["updated"]),
                "deleted": len(report["deleted"])
            },
            "details": {
                "created": report["created"],
                "updated": detailed_updates,
                "deleted": report["deleted"]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========================================
# ENDPOINTS RESTORE
# ========================================

@app.post("/restore/{snapshot_id}")
def restore_snapshot(
    snapshot_id: int,
    skip_checks: bool = False,
    selective: bool = True
):
    """Restaure un snapshot"""
    
    try:
        restore_engine = RestoreEngine(snapshot_id=snapshot_id)
        
        if selective:
            report = restore_engine.run_smart_restore_selective(skip_checks=skip_checks)
        else:
            report = restore_engine.run_smart_restore(skip_checks=skip_checks)
        
        return {
            "snapshot_id": snapshot_id,
            "success": True,
            "report": report
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========================================
# ENDPOINTS STATS
# ========================================

@app.get("/stats")
def get_stats(session: Session = Depends(get_session)):
    """Statistiques globales"""
    
    # Nombre total de snapshots
    total_snapshots = session.exec(select(func.count()).select_from(Snapshot)).one()
    
    # Dernier snapshot
    latest_snapshot = session.exec(
        select(Snapshot).order_by(Snapshot.id.desc())
    ).first()
    
    # Nombre total d'items dans le dernier snapshot
    total_items = 0
    items_by_type = {}
    
    if latest_snapshot:
        items_stmt = select(SnapshotItem).where(
            SnapshotItem.snapshot_id == latest_snapshot.id
        )
        items = session.exec(items_stmt).all()
        
        for item in items:
            total_items += 1
            if item.object_type not in items_by_type:
                items_by_type[item.object_type] = 0
            items_by_type[item.object_type] += 1
    
    return {
        "total_snapshots": total_snapshots,
        "total_items": total_items,
        "items_by_type": items_by_type,
        "latest_snapshot": {
            "id": latest_snapshot.id,
            "timestamp": latest_snapshot.timestamp.isoformat() if latest_snapshot and hasattr(latest_snapshot.timestamp, 'isoformat') else None
        } if latest_snapshot else None
    }

# ========================================
# HEALTH CHECK
# ========================================

@app.get("/")
def root():
    return {
        "name": "Zibridge API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
def health_check():
    return {"status": "healthy"}