# src/core/restore.py - VERSION OPTIMISÉE

from typing import List, Optional, Dict, Any, Set
from loguru import logger
from sqlmodel import Session, select
from src.core.models import SnapshotProject, IdMapping, SnapshotItem, Snapshot
from src.utils.db import engine, storage_manager
from src.core.snapshot import SnapshotEngine
from src.core.hashing import calculate_content_hash
from src.core.graph import GraphManager
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.panel import Panel

console = Console()

class RestoreEngine:
    def __init__(self, project_id: int, connector: Any, snapshot_id: Optional[int] = None, dry_run: bool = False):
        self.project_id = project_id
        self.snapshot_id = snapshot_id
        self.connector = connector
        self.dry_run = dry_run
        self.graph = GraphManager()
        
        if snapshot_id:
            self.snap_engine = SnapshotEngine(snapshot_id=snapshot_id)
        
        with Session(engine) as session:
            self.project = session.get(SnapshotProject, project_id)
        
        # Stats détaillées
        self.stats = {
            "objects_created": 0,
            "objects_updated": 0,
            "objects_deleted": 0,
            "objects_unchanged": 0,
            "relations_added": 0,
            "relations_removed": 0,
            "errors": 0,
            "details": {
                "created": [],
                "updated": [],
                "deleted": [],
                "relations_restored": []
            }
        }

    def get_preflight_report(self) -> Dict[str, Any]:
        """
        🛫 Analyse PRÉ-VOL OPTIMISÉE : Compare snapshot vs CRM sans appels API inutiles.
        """
        logger.info(f"🔍 Analyse pré-vol optimisée : Snapshot #{self.snapshot_id} vs CRM actuel")
        
        actions = {
            "to_create": [],
            "to_update": [],
            "to_delete": [],
            "relations_to_add": [],
            "relations_to_remove": []
        }
        
        # Récupérer tous les types d'objets
        object_types = self.connector.get_available_object_types()
        
        for obj_type in object_types:
            logger.info(f"📊 Analyse {obj_type}...")
            
            # 1. Récupérer les objets du SNAPSHOT
            with Session(engine) as session:
                snapshot_items = session.exec(
                    select(SnapshotItem).where(
                        SnapshotItem.snapshot_id == self.snapshot_id,
                        SnapshotItem.object_type == obj_type
                    )
                ).all()
            
            # Créer un dictionnaire {id: hash} pour le snapshot
            snapshot_map = {}
            snapshot_data = {}
            
            for item in snapshot_items:
                snapshot_map[item.object_id] = item.content_hash
                # Charger les données complètes
                data = storage_manager.get_json(f"blobs/{item.content_hash}.json")
                snapshot_data[item.object_id] = data
            
            # 2. Récupérer les objets du CRM ACTUEL (avec associations déjà incluses)
            logger.info(f"🔄 Extraction {obj_type} depuis le CRM...")
            current_items = list(self.connector.extract_data(obj_type))
            current_map = {}
            
            for item in current_items:
                obj_id = str(item.get("id", item.get("hs_object_id")))
                
                # ✅ Les associations sont DÉJÀ dans item["_zibridge_links"]
                # Pas besoin d'appel API supplémentaire !
                current_map[obj_id] = item
            
            logger.success(f"✅ {len(current_items)} {obj_type} récupérés du CRM")
            
            # 3. DÉTECTER LES DIFFÉRENCES
            
            # A. Objets à CRÉER (dans snapshot, pas dans CRM)
            for snap_id in snapshot_map.keys():
                if snap_id not in current_map:
                    actions["to_create"].append({
                        "type": obj_type,
                        "id": snap_id
                    })
            
            # B. Objets à SUPPRIMER (dans CRM, pas dans snapshot)
            for crm_id in current_map.keys():
                if crm_id not in snapshot_map:
                    actions["to_delete"].append({
                        "type": obj_type,
                        "id": crm_id
                    })
            
            # C. Objets à MODIFIER (dans les deux, mais différents)
            for snap_id, snap_hash in snapshot_map.items():
                if snap_id in current_map:
                    current_item = current_map[snap_id]
                    
                    # Calculer le hash de l'objet actuel du CRM
                    current_hash = calculate_content_hash(current_item, include_links=True)
                    
                    # Si les hash sont différents, analyser en détail
                    if snap_hash != current_hash:
                        snap_data = snapshot_data[snap_id]
                        
                        # Comparer les propriétés
                        snap_props = snap_data.get("properties", snap_data)
                        current_props = {k: v for k, v in current_item.items() if not k.startswith("_")}
                        
                        property_changes = {}
                        for key in set(snap_props.keys()) | set(current_props.keys()):
                            if key.startswith("_") or key in ["id", "hs_object_id", "createdate", "lastmodifieddate", "hs_lastmodifieddate"]:
                                continue
                            
                            snap_val = snap_props.get(key)
                            current_val = current_props.get(key)
                            
                            if str(snap_val) != str(current_val):
                                property_changes[key] = {
                                    "old": current_val,
                                    "new": snap_val
                                }
                        
                        # Comparer les relations
                        snap_links = snap_data.get("_zibridge_links", {})
                        current_links = current_item.get("_zibridge_links", {})
                        
                        snap_links_set = self._normalize_links(snap_links)
                        current_links_set = self._normalize_links(current_links)
                        
                        relations_to_add = snap_links_set - current_links_set
                        relations_to_remove = current_links_set - snap_links_set
                        
                        # Si il y a des changements, ajouter à la liste
                        if property_changes or relations_to_add or relations_to_remove:
                            actions["to_update"].append({
                                "type": obj_type,
                                "id": snap_id,
                                "property_changes": property_changes,
                                "relations_to_add": sorted(list(relations_to_add)),
                                "relations_to_remove": sorted(list(relations_to_remove))
                            })
                        
                        # Ajouter les relations dans les listes globales
                        for rel in relations_to_add:
                            actions["relations_to_add"].append({
                                "from_type": obj_type,
                                "from_id": snap_id,
                                "relation": rel
                            })
                        
                        for rel in relations_to_remove:
                            actions["relations_to_remove"].append({
                                "from_type": obj_type,
                                "from_id": snap_id,
                                "relation": rel
                            })
        
        # Créer le résumé
        summary = {
            "created": len(actions["to_create"]),
            "updated": len(actions["to_update"]),
            "deleted": len(actions["to_delete"]),
            "unchanged": 0
        }
        
        analysis = {
            "snapshot_id": self.snapshot_id,
            "summary": summary,
            "restore_actions": actions,
            "warnings": self._detect_warnings_from_actions(actions)
        }
        
        logger.success(f"✅ Analyse terminée : {summary['created']} créés, {summary['updated']} modifiés, {summary['deleted']} supprimés")
        
        return analysis

    def _normalize_links(self, links: Any) -> set:
        """🔗 Normalise les liens vers un set de strings 'type:id'."""
        result = set()
        
        if not links:
            return result
        
        # Format dict
        if isinstance(links, dict):
            for rel_type, ids in links.items():
                id_list = ids if isinstance(ids, list) else [ids]
                for rel_id in id_list:
                    result.add(f"{rel_type}:{rel_id}")
        
        # Format list[dict]
        elif isinstance(links, list):
            for item in links:
                if isinstance(item, dict) and "id" in item:
                    rel_type = item.get("type", "unknown")
                    result.add(f"{rel_type}:{item['id']}")
                elif isinstance(item, str):
                    result.add(item)
        
        return result

    def _detect_warnings_from_actions(self, actions: Dict) -> List[str]:
        """⚠️ Détecte les avertissements potentiels."""
        warnings = []
        
        if len(actions["to_delete"]) > 0:
            warnings.append(f"⚠️ {len(actions['to_delete'])} objet(s) sera/seront supprimé(s) du CRM")
        
        if len(actions["to_update"]) > 50:
            warnings.append(f"⚠️ {len(actions['to_update'])} objets seront modifiés")
        
        if len(actions["to_create"]) > 100:
            warnings.append(f"⚠️ {len(actions['to_create'])} objets seront créés")
        
        return warnings

    def display_preflight(self, analysis: Dict):
        """📊 Affiche le rapport pré-vol détaillé."""
        console.print("\n")
        console.print(Panel.fit(
            f"[bold cyan]📋 RAPPORT DE RESTAURATION[/bold cyan]\n"
            f"Snapshot cible : #{analysis['snapshot_id']}",
            border_style="cyan"
        ))
        
        actions = analysis["restore_actions"]
        
        # Table résumé
        table = Table(title="Impact de la restauration", show_header=True)
        table.add_column("Action", style="cyan", width=30)
        table.add_column("Quantité", justify="right", style="yellow", width=10)
        table.add_column("Détail", style="dim", width=40)
        
        table.add_row(
            "✅ Objets à créer",
            str(len(actions["to_create"])),
            "Objets manquants dans le CRM"
        )
        table.add_row(
            "📝 Objets à modifier",
            str(len(actions["to_update"])),
            "Propriétés ou relations différentes"
        )
        table.add_row(
            "❌ Objets à supprimer",
            str(len(actions["to_delete"])),
            "Objets en trop dans le CRM"
        )
        table.add_row(
            "🔗 Relations à ajouter",
            str(len(actions["relations_to_add"])),
            "Associations manquantes"
        )
        table.add_row(
            "🔴 Relations à supprimer",
            str(len(actions["relations_to_remove"])),
            "Associations en trop"
        )
        
        console.print(table)
        
        # Avertissements
        if analysis.get("warnings"):
            console.print("\n[bold yellow]⚠️ AVERTISSEMENTS :[/bold yellow]")
            for warning in analysis["warnings"]:
                console.print(f"  {warning}")
        
        # Détails des objets à créer (limité à 10)
        if actions["to_create"]:
            count = len(actions["to_create"])
            console.print(f"\n[bold green]✅ Objets à créer : {count}[/bold green]")
            for obj in actions["to_create"][:10]:
                console.print(f"  • {obj['type']} #{obj['id']}")
            if count > 10:
                console.print(f"  [dim]... et {count - 10} autres[/dim]")
        
        # Détails des objets à modifier (limité à 10)
        if actions["to_update"]:
            count = len(actions["to_update"])
            console.print(f"\n[bold yellow]📝 Objets à modifier : {count}[/bold yellow]")
            for obj in actions["to_update"][:10]:
                details = []
                if obj.get("property_changes"):
                    details.append(f"{len(obj['property_changes'])} propriété(s)")
                if obj.get("relations_to_add"):
                    details.append(f"+{len(obj['relations_to_add'])} relation(s)")
                if obj.get("relations_to_remove"):
                    details.append(f"-{len(obj['relations_to_remove'])} relation(s)")
                
                console.print(f"  • {obj['type']} #{obj['id']} : {', '.join(details)}")
            if count > 10:
                console.print(f"  [dim]... et {count - 10} autres[/dim]")
        
        # Détails des objets à supprimer (limité à 10)
        if actions["to_delete"]:
            count = len(actions["to_delete"])
            console.print(f"\n[bold red]❌ Objets à supprimer : {count}[/bold red]")
            for obj in actions["to_delete"][:10]:
                console.print(f"  • {obj['type']} #{obj['id']}")
            if count > 10:
                console.print(f"  [dim]... et {count - 10} autres[/dim]")

    def run(self) -> Dict[str, Any]:
        """
        🚀 Exécute la restauration complète.
        """
        logger.info(f"🔄 Démarrage restauration Snapshot #{self.snapshot_id}")
        
        if self.dry_run:
            logger.warning("🧪 MODE SIMULATION - Aucune modification réelle")
            console.print("[bold yellow]🧪 MODE SIMULATION ACTIVÉ[/bold yellow]\n")
        
        # 1. Récupérer le plan de restauration
        analysis = self.get_preflight_report()
        actions = analysis["restore_actions"]
        
        # 2. Exécution avec barre de progression
        total_operations = (
            len(actions["to_create"]) +
            len(actions["to_update"]) +
            len(actions["relations_to_add"])
        )
        
        if total_operations == 0:
            console.print("[bold green]✅ Aucune modification nécessaire - Le CRM est déjà à jour ![/bold green]")
            return self._build_final_report()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console
        ) as progress:
            
            task = progress.add_task("[cyan]Restauration en cours...", total=total_operations)
            
            # Phase 1 : Création d'objets
            if actions["to_create"]:
                progress.update(task, description="[green]Création d'objets...")
                self._create_objects(actions["to_create"], progress, task)
            
            # Phase 2 : Mise à jour d'objets
            if actions["to_update"]:
                progress.update(task, description="[yellow]Mise à jour d'objets...")
                self._update_objects(actions["to_update"], progress, task)
            
            # Phase 3 : Restauration des relations
            if actions["relations_to_add"]:
                progress.update(task, description="[blue]Restauration des relations...")
                self._restore_relations(actions["relations_to_add"], progress, task)
        
        # 3. Rapport final
        return self._build_final_report()

    def _create_objects(self, objects: List[Dict], progress: Progress, task):
        """✅ Crée les objets manquants dans le CRM."""
        for obj in objects:
            try:
                # Récupérer les données depuis le snapshot
                with Session(engine) as session:
                    item = session.exec(
                        select(SnapshotItem).where(
                            SnapshotItem.snapshot_id == self.snapshot_id,
                            SnapshotItem.object_type == obj["type"],
                            SnapshotItem.object_id == obj["id"]
                        )
                    ).first()
                    
                    if not item:
                        logger.warning(f"⚠️ Objet {obj['type']} #{obj['id']} introuvable")
                        progress.update(task, advance=1)
                        continue
                    
                    # Charger les données du blob
                    data = storage_manager.get_json(f"blobs/{item.content_hash}.json")
                    properties = data.get("properties", data)
                    
                    # Nettoyer les propriétés système
                    clean_props = {
                        k: v for k, v in properties.items()
                        if not k.startswith("_") and k not in ["id", "hs_object_id", "createdate", "lastmodifieddate", "hs_lastmodifieddate"]
                    }
                    
                    if not self.dry_run:
                        # Créer dans le CRM
                        new_obj = self.connector.push_create(obj["type"], clean_props)
                        logger.success(f"✅ Créé : {obj['type']} #{obj['id']} → #{new_obj.get('id')}")
                        
                        self.stats["details"]["created"].append({
                            "type": obj["type"],
                            "old_id": obj["id"],
                            "new_id": new_obj.get("id")
                        })
                    else:
                        logger.info(f"[DRY-RUN] Création : {obj['type']} #{obj['id']}")
                    
                    self.stats["objects_created"] += 1
                    
            except Exception as e:
                logger.error(f"❌ Erreur création {obj['type']} #{obj['id']}: {e}")
                self.stats["errors"] += 1
            
            progress.update(task, advance=1)

    def _update_objects(self, objects: List[Dict], progress: Progress, task):
        """📝 Met à jour les objets modifiés."""
        for obj in objects:
            try:
                # Ne mettre à jour QUE les propriétés qui ont changé
                if obj.get("property_changes"):
                    changed_props = {}
                    for prop, values in obj["property_changes"].items():
                        changed_props[prop] = values["new"]
                    
                    if changed_props and not self.dry_run:
                        self.connector.push_update(obj["type"], obj["id"], changed_props)
                        logger.success(f"📝 Mis à jour : {obj['type']} #{obj['id']}")
                    elif changed_props:
                        logger.info(f"[DRY-RUN] Mise à jour : {obj['type']} #{obj['id']}")
                
                self.stats["objects_updated"] += 1
                self.stats["details"]["updated"].append({
                    "type": obj["type"],
                    "id": obj["id"],
                    "changes": obj.get("property_changes", {})
                })
                    
            except Exception as e:
                logger.error(f"❌ Erreur mise à jour {obj['type']} #{obj['id']}: {e}")
                self.stats["errors"] += 1
            
            progress.update(task, advance=1)

    def _restore_relations(self, relations: List[Dict], progress: Progress, task):
        """🔗 Restaure les relations manquantes."""
        for rel_info in relations:
            try:
                # Parser la relation "type:id"
                relation_str = rel_info["relation"]
                if ":" in relation_str:
                    to_type, to_id = relation_str.split(":", 1)
                else:
                    logger.warning(f"⚠️ Format de relation invalide : {relation_str}")
                    progress.update(task, advance=1)
                    continue
                
                if not self.dry_run:
                    # Créer l'association dans le CRM
                    self.connector.create_association(
                        from_type=rel_info["from_type"],
                        from_id=rel_info["from_id"],
                        to_type=to_type,
                        to_id=to_id
                    )
                    logger.success(f"🔗 Relation : {rel_info['from_type']}:{rel_info['from_id']} → {to_type}:{to_id}")
                else:
                    logger.info(f"[DRY-RUN] Relation : {rel_info['from_type']}:{rel_info['from_id']} → {to_type}:{to_id}")
                
                self.stats["relations_added"] += 1
                self.stats["details"]["relations_restored"].append({
                    "from": f"{rel_info['from_type']}:{rel_info['from_id']}",
                    "to": relation_str
                })
                
            except Exception as e:
                logger.error(f"❌ Erreur relation {rel_info}: {e}")
                self.stats["errors"] += 1
            
            progress.update(task, advance=1)

    def _build_final_report(self) -> Dict[str, Any]:
        """📊 Construit le rapport final de restauration."""
        total_operations = (
            self.stats["objects_created"] +
            self.stats["objects_updated"] +
            self.stats["relations_added"]
        )
        
        report = {
            "success": total_operations - self.stats["errors"],
            "failed": self.stats["errors"],
            "total": total_operations,
            "breakdown": {
                "objects_created": self.stats["objects_created"],
                "objects_updated": self.stats["objects_updated"],
                "relations_restored": self.stats["relations_added"]
            },
            "details": self.stats["details"]
        }
        
        return report