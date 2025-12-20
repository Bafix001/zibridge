from loguru import logger
from sqlmodel import Session, select
from src.connectors.rest_api import RestApiConnector
from src.core.snapshot import SnapshotEngine
from src.core.graph import GraphManager
from src.core.models import IdMapping, Snapshot
from src.utils.db import engine
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

class RestoreEngine:
    def __init__(self, snapshot_id: int):
        self.snapshot_id = snapshot_id
        self.connector = RestApiConnector()
        self.snap_engine = SnapshotEngine(snapshot_id=snapshot_id)
        self.graph = GraphManager()
        self.id_mapping = {}  # Cache en mémoire : {old_id: new_id}

    def _get_display_name(self, obj_type: str, item: dict) -> str:
        """Extrait un nom lisible depuis les données."""
        props = item.get("properties", item)
        if obj_type == "companies":
            return props.get("name", "Sans nom")
        elif obj_type == "contacts":
            first = props.get("firstname", "")
            last = props.get("lastname", "")
            return f"{first} {last}".strip() or "Sans nom"
        elif obj_type == "deals":
            return props.get("dealname", "Sans nom")
        return "Inconnu"

    def _extract_id(self, item: dict, obj_type: str) -> str:
        """Extrait l'ID d'un objet."""
        props = item.get("properties", item)
        return str(item.get("id") or props.get("hs_object_id") or item.get(f"{obj_type[:-1]}Id"))

    def _get_current_entities(self, object_type: str) -> set:
        """Récupère les IDs de tous les objets actuellement dans le CRM."""
        current_ids = set()
        try:
            for item in self.connector.extract_data(object_type):
                item_id = str(item.get("id"))
                if item_id:
                    current_ids.add(item_id)
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer les entités actuelles {object_type}: {e}")
        return current_ids

    def analyze_restore_impact(self, object_type: str, external_id: str) -> dict:
        """Analyse l'impact d'une restauration AVANT de l'exécuter."""
        impact = self.graph.get_impact_analysis(object_type, external_id, self.snapshot_id)
        warnings = []
        all_current_entities = set()
        for rel_type in impact["historical_relations"].keys():
            current = self._get_current_entities(rel_type)
            all_current_entities.update(current)
        orphans = self.graph.check_orphans(object_type, external_id, self.snapshot_id, all_current_entities)
        if orphans:
            for missing_type, missing_ids in orphans.items():
                entity_type = missing_type.replace("missing_", "")
                warnings.append(
                    f"⚠️ {len(missing_ids)} {entity_type} liés n'existent plus : {', '.join(['#' + id for id in missing_ids[:3]])}"
                )
        safe = len(warnings) == 0
        return {"safe": safe, "warnings": warnings, "impact": impact, "orphans": orphans}

    def display_impact_warning(self, object_type: str, external_id: str, display_name: str, analysis: dict):
        """Affiche un panneau d'alerte visuel avec Rich."""
        if analysis["safe"]:
            return
        table = Table(title="⚠️ Relations Manquantes", show_header=True, header_style="bold red")
        table.add_column("Type", style="cyan")
        table.add_column("IDs Manquants", style="yellow")
        for missing_type, missing_ids in analysis["orphans"].items():
            entity_type = missing_type.replace("missing_", "")
            ids_str = ", ".join([f"#{id}" for id in missing_ids[:5]])
            if len(missing_ids) > 5:
                ids_str += f" ... (+{len(missing_ids) - 5})"
            table.add_row(entity_type.upper(), ids_str)
        graph_visual = self.graph.visualize_entity_graph(object_type, external_id, self.snapshot_id)
        warning_text = f"""
[bold yellow]⚠️ ALERTE DE COHÉRENCE RELATIONNELLE[/bold yellow]
Objet : [cyan]{object_type} #{external_id} ({display_name})[/cyan]
Snapshot : [magenta]#{self.snapshot_id}[/magenta]
[bold red]Problème détecté :[/bold red]
Cet objet avait {analysis['impact']['relation_count']} relations dans le snapshot,
mais certaines entités liées n'existent plus dans le CRM actuel.
[bold]Graphe historique :[/bold]
{graph_visual}
        """
        console.print(Panel(warning_text, border_style="red", expand=False))
        console.print(table)
        console.print("\n[yellow]La restauration peut créer des incohérences dans votre CRM.[/yellow]")
        console.print("[yellow]Recommandation : Restaurez d'abord les entités liées manquantes.[/yellow]\n")

    def _get_association_type_id(self, from_type: str, to_type: str) -> int:
        """Retourne le type d'association HubSpot."""
        association_map = {
            ("contacts", "companies"): 1,
            ("companies", "contacts"): 2,
            ("deals", "contacts"): 3,
            ("deals", "companies"): 5,
            ("tickets", "contacts"): 16,
            ("tickets", "companies"): 26,
        }
        return association_map.get((from_type, to_type), 1)

    def _restore_associations(self, object_type: str, old_id: str, current_id: str, item_data: dict = None):
        """Recrée les associations dans HubSpot après restauration (Hybride JSON/Graphe)."""
        relations = {}
        # 1. Extraction depuis les liens injectés dans le JSON (Nouveaux snapshots)
        if item_data and "_zibridge_links" in item_data:
            links = item_data["_zibridge_links"]
            if "company_id" in links:
                relations.setdefault("companies", []).append(links["company_id"])
            if "contact_id" in links:
                relations.setdefault("contacts", []).append(links["contact_id"])
        
        # 2. Repli sur Neo4j si aucune donnée JSON
        if not relations:
            relations = self.graph.get_entity_relations(object_type, old_id, self.snapshot_id)
        
        if not relations:
            logger.debug(f"Aucune association à restaurer pour {object_type}/{old_id}")
            return
        
        associations_count = 0
        for related_type, related_ids in relations.items():
            for old_related_id in related_ids:
                mapping_key = f"{related_type}/{old_related_id}"
                actual_related_id = self.id_mapping.get(mapping_key, old_related_id)
                
                assoc_type_id = self._get_association_type_id(object_type, related_type)
                success = self.connector.create_association(
                    from_type=object_type, from_id=current_id,
                    to_type=related_type, to_id=actual_related_id,
                    association_type_id=assoc_type_id
                )
                if success:
                    logger.success(f"🔗 Lien restauré : {object_type}/{current_id} → {related_type}/{actual_related_id}")
                    associations_count += 1
        
        if associations_count > 0:
            logger.info(f"✨ {associations_count} associations rétablies pour cet objet.")

    def _save_id_mapping(self, object_type: str, old_id: str, new_id: str):
        """Sauvegarde le mapping old_id → new_id en DB et en cache."""
        self.id_mapping[f"{object_type}/{old_id}"] = new_id
        with Session(engine) as session:
            mapping = IdMapping(snapshot_id=self.snapshot_id, object_type=object_type, old_id=old_id, new_id=new_id)
            session.add(mapping)
            session.commit()
            logger.debug(f"💾 Mapping sauvegardé: {object_type}/{old_id} → {new_id}")

    def run_smart_restore_selective(self, skip_checks: bool = False):
        """Restauration SÉLECTIVE : Uniquement changements + Auto-Suture."""
        with Session(engine) as session:
            latest_snap = session.exec(select(Snapshot).order_by(Snapshot.id.desc())).first()
            if not latest_snap: return {"success": 0, "failed": 0}
            current_snap_id = latest_snap.id
        
        if current_snap_id == self.snapshot_id:
            logger.success("✅ État déjà conforme au snapshot cible.")
            return {"success": 0, "failed": 0}
        
        from src.core.diff import DiffEngine
        diff_report = DiffEngine(self.snapshot_id, current_snap_id).generate_report()
        
        objects_to_restore = {}
        for item in diff_report["deleted"] + diff_report["updated"]:
            obj_type = item["type"]
            if obj_type not in objects_to_restore: objects_to_restore[obj_type] = set()
            objects_to_restore[obj_type].add(item["id"])
        
        total = sum(len(ids) for ids in objects_to_restore.values())
        if total == 0: return {"success": 0, "failed": 0}
        
        logger.warning(f"🚨 DÉBUT DU ROLLBACK SÉLECTIF VERS #{self.snapshot_id}")
        report = {"success": 0, "failed": 0, "resurrected": 0, "merged": 0, "warnings": 0, "skipped": 0}
        
        for obj_type in ["companies", "contacts", "deals"]:
            if obj_type not in objects_to_restore: continue
            ids_to_res = objects_to_restore[obj_type]
            try:
                all_data = self.snap_engine.get_all_items_from_minio(obj_type)
                for item in all_data:
                    ext_id = self._extract_id(item, obj_type)
                    if ext_id not in ids_to_res:
                        report["skipped"] += 1
                        continue
                    
                    display_name = self._get_display_name(obj_type, item)
                    if not skip_checks:
                        analysis = self.analyze_restore_impact(obj_type, ext_id)
                        if not analysis["safe"]:
                            self.display_impact_warning(obj_type, ext_id, display_name, analysis)
                            report["warnings"] += 1
                            from rich.prompt import Confirm
                            if not Confirm.ask(f"[yellow]Continuer la restauration de {display_name} ?[/yellow]"): continue
                    
                    logger.info(f"🔄 Restauration de {obj_type} #{ext_id} ({display_name})...")
                    status, new_id = self.connector.push_update(obj_type, ext_id, item)
                    target_id = new_id if new_id else ext_id
                    
                    if status in ["updated", "resurrected", "merged"]:
                        if status != "updated": self._save_id_mapping(obj_type, ext_id, target_id)
                        self._restore_associations(obj_type, ext_id, target_id, item)
                        report[status if status != "updated" else "success"] += 1
                        if status == "updated": report["success"] += 1
                    else:
                        report["failed"] += 1
            except Exception as e:
                logger.error(f"❌ Erreur {obj_type}: {e}")
        return report

    def run_smart_restore(self, skip_checks: bool = False):
        """Restauration complète avec ordre de dépendances et Auto-Suture."""
        logger.warning(f"🚨 DÉBUT DU ROLLBACK INTELLIGENT VERS LE SNAPSHOT #{self.snapshot_id}")
        report = {"success": 0, "failed": 0, "resurrected": 0, "merged": 0, "warnings": 0}
        
        for obj_type in ["companies", "contacts", "deals"]:
            logger.info(f"\n📦 === PHASE : Restauration {obj_type.upper()} ===")
            try:
                data_to_restore = self.snap_engine.get_all_items_from_minio(obj_type)
                for item in data_to_restore:
                    ext_id = self._extract_id(item, obj_type)
                    if not ext_id: continue
                    display_name = self._get_display_name(obj_type, item)
                    
                    if not skip_checks:
                        analysis = self.analyze_restore_impact(obj_type, ext_id)
                        if not analysis["safe"]:
                            self.display_impact_warning(obj_type, ext_id, display_name, analysis)
                            report["warnings"] += 1
                            from rich.prompt import Confirm
                            if not Confirm.ask(f"[yellow]Continuer la restauration de {display_name} ?[/yellow]"): continue
                    
                    logger.info(f"🔄 Restauration de {obj_type} #{ext_id} ({display_name})...")
                    status, new_id = self.connector.push_update(obj_type, ext_id, item)
                    target_id = new_id if new_id else ext_id
                    
                    if status in ["updated", "resurrected", "merged"]:
                        if status != "updated": self._save_id_mapping(obj_type, ext_id, target_id)
                        self._restore_associations(obj_type, ext_id, target_id, item)
                        report[status if status != "updated" else "success"] += 1
                        if status == "updated": report["success"] += 1
                    else:
                        report["failed"] += 1
            except Exception as e:
                logger.error(f"❌ Erreur {obj_type}: {e}")
        return report

    def run_full_restore(self, object_types: list = ["companies", "contacts", "deals"], target_only: str = None, skip_checks: bool = False):
        """Restauration classique conservée pour compatibilité."""
        filter_type, filter_id = None, None
        if target_only and "/" in target_only:
            filter_type, filter_id = target_only.split("/")
            logger.info(f"🎯 Cible spécifique détectée : {target_only}")

        logger.warning(f"🚨 DÉBUT DU ROLLBACK VERS LE SNAPSHOT #{self.snapshot_id}")
        report = {"success": 0, "failed": 0, "resurrected": 0, "merged": 0, "warnings": 0}

        for obj_type in object_types:
            if filter_type and obj_type != filter_type: continue
            try:
                data_to_restore = self.snap_engine.get_all_items_from_minio(obj_type)
                for item in data_to_restore:
                    ext_id = self._extract_id(item, obj_type)
                    if not ext_id or (filter_id and ext_id != str(filter_id)): continue
                    display_name = self._get_display_name(obj_type, item)
                    
                    if not skip_checks:
                        analysis = self.analyze_restore_impact(obj_type, ext_id)
                        if not analysis["safe"]:
                            self.display_impact_warning(obj_type, ext_id, display_name, analysis)
                            report["warnings"] += 1
                            from rich.prompt import Confirm
                            if not Confirm.ask(f"[yellow]Continuer la restauration de {display_name} ?[/yellow]"): continue
                    
                    logger.info(f"🔄 Restauration de {obj_type} #{ext_id} ({display_name})...")
                    status, new_id = self.connector.push_update(obj_type, ext_id, item)
                    target_id = new_id if new_id else ext_id

                    if status in ["updated", "resurrected", "merged"]:
                        if status != "updated": self._save_id_mapping(obj_type, ext_id, target_id)
                        self._restore_associations(obj_type, ext_id, target_id, item)
                        report[status if status != "updated" else "success"] += 1
                        if status == "updated": report["success"] += 1
                    else:
                        report["failed"] += 1
            except Exception as e:
                logger.error(f"❌ Erreur {obj_type}: {e}")
        return report