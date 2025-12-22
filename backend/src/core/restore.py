import time
from typing import List, Optional, Dict, Any
from loguru import logger
from sqlmodel import Session, select
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm

from src.connectors.base import BaseConnector
from src.core.snapshot import SnapshotEngine
from src.core.graph import GraphManager
from src.core.models import IdMapping, Snapshot
from src.utils.db import engine

console = Console()

class RestoreEngine:
    def __init__(self, snapshot_id: int, connector: BaseConnector, dry_run: bool = False):
        self.snapshot_id = snapshot_id
        self.connector = connector
        self.dry_run = dry_run
        self.snap_engine = SnapshotEngine(snapshot_id=snapshot_id)
        self.graph = GraphManager()
        self.id_mapping = {}

        if self.dry_run:
            console.print(Panel(
                f"[bold magenta]🧪 MODE SIMULATION (DRY-RUN) ACTIVÉ[/bold magenta]\n"
                f"Connecteur : [white]{type(self.connector).__name__}[/white] | Source : [cyan]{self.connector.source_type}[/cyan]",
                border_style="magenta"
            ))

    def _apply_rate_limit(self):
        """⏳ Applique un délai uniquement pour les sources de type API."""
        if not self.dry_run and self.connector.source_type == "api":
            time.sleep(0.2)

    def _get_display_name(self, object_type: str, item: Dict[str, Any]) -> str:
        """Récupère un nom lisible de manière agnostique (plat ou imbriqué)."""
        # On fusionne properties et racine pour chercher les clés de nommage
        props = item.get("properties", item)
        name_keys = ["name", "subject", "label", "title", "lastname", "email", "full_name"]
        
        for key in name_keys:
            if props.get(key): return str(props[key])
        return f"{object_type} #{item.get('id', 'unknown')}"

    def push_data(self, object_type: str, item_id: str, snapshot_item: Dict[str, Any], prop_filter: List[str] = None):
        """Pousse les données en respectant le schéma de la source (API ou Fichier)."""
        
        # 1. Extraction intelligente des données (Gère HubSpot 'properties' vs CSV 'flat')
        full_data = snapshot_item.get("properties", snapshot_item)
        
        # 2. Filtrage chirurgical
        if prop_filter:
            clean_data = {k: v for k, v in full_data.items() if k in prop_filter}
        else:
            # On retire les métadonnées internes Zibridge avant le push
            clean_data = {k: v for k, v in full_data.items() if not k.startswith("_zibridge")}

        if self.dry_run:
            logger.info(f"🧪 [SIMUL] {self.connector.source_type.upper()} Push -> {object_type}/{item_id}")
            return "updated", item_id

        self._apply_rate_limit()
        
        # 3. Le connecteur décide comment 'pousser' (Update API ou écriture ligne CSV)
        # On envoie la donnée 'propre' au connecteur
        return self.connector.push_update(object_type, item_id, clean_data)

    def _restore_associations(self, object_type: str, old_id: str, current_id: str, item_data: Dict[str, Any]):
        """Suture universelle des liens."""
        if self.dry_run: return

        # On privilégie les liens stockés dans le JSON (plus fiable pour les fichiers)
        relations = item_data.get("_zibridge_links", {})
        if not relations:
            relations = self.graph.get_entity_relations(object_type, old_id, self.snapshot_id)

        for rel_type, rel_ids in relations.items():
            target_ids = rel_ids if isinstance(rel_ids, list) else [rel_ids]
            for o_rel_id in target_ids:
                # Mapping dynamique (si l'objet lié a changé d'ID pendant cette session)
                actual_rel_id = self.id_mapping.get(f"{rel_type}/{o_rel_id}", o_rel_id)
                
                assoc_def = self.connector.get_association_definition(object_type, rel_type)
                
                if assoc_def:
                    self.connector.create_association(
                        from_type=object_type, from_id=current_id,
                        to_type=rel_type, to_id=actual_rel_id,
                        assoc_type_id=assoc_def
                    )

    def run_smart_restore(self, selected_props: List[str] = None, skip_checks: bool = False):
        """Cycle de restauration universel."""
        # On pourrait définir l'ordre de priorité dans le connecteur
        priority_order = ["companies", "contacts", "deals", "tickets"]
        report = {"success": 0, "failed": 0, "resurrected": 0, "merged": 0}

        for obj_type in priority_order:
            console.rule(f"[bold]{obj_type.upper()}[/bold]")
            items = self.snap_engine.get_all_items_from_minio(obj_type)
            
            for item in items:
                ext_id = str(item.get("id"))
                name = self._get_display_name(obj_type, item)

                # Push & Suture
                status, new_id = self.push_data(obj_type, ext_id, item, prop_filter=selected_props)
                target_id = new_id if new_id else ext_id

                if status in ["updated", "resurrected", "merged"]:
                    if status != "updated" and not self.dry_run:
                        self._save_id_mapping(obj_type, ext_id, target_id)
                    
                    self._restore_associations(obj_type, ext_id, target_id, item)
                    report[status if status != "updated" else "success"] += 1
                else:
                    report["failed"] += 1

        return report

    def _save_id_mapping(self, obj_type, old_id, new_id):
        self.id_mapping[f"{obj_type}/{old_id}"] = new_id
        with Session(engine) as session:
            session.add(IdMapping(
                snapshot_id=self.snapshot_id, 
                object_type=obj_type, 
                old_id=old_id, 
                new_id=new_id
            ))
            session.commit()