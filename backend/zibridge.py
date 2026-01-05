import typer
import sys
import os
import warnings
from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm
from sqlmodel import Session, select
from typing import Optional
from dotenv import load_dotenv

# 🔥 Configuration environnement
load_dotenv(os.path.join(os.getcwd(), ".env"))
sys.path.append(os.getcwd())

sys.path.append(os.path.join(os.getcwd(), "scripts"))

# Imports internes
# On utilise l'import direct de la fonction de synchro pour éviter les erreurs de chemin subprocess
from scripts.run_sync import run_sync as execute_sync
from src.core.diff import DiffEngine
from src.core.restore import RestoreEngine
from src.utils.db import engine
from src.core.models import Snapshot, SnapshotProject, Branch

# Suppression des warnings polluants
warnings.filterwarnings("ignore")

app = typer.Typer(help="🚀 Zibridge CLI - Système de Versioning pour CRM (Elon Mode)")
console = Console()

# ==============================================================================
# 1. GESTION DES PROJETS & BRANCHES
# ==============================================================================

@app.command()
def projects():
    """Liste tous les projets configurés."""
    with Session(engine) as session:
        projs = session.exec(select(SnapshotProject)).all()
        table = Table(title="📂 Projets Zibridge")
        table.add_column("ID", style="cyan")
        table.add_column("Nom", style="green")
        table.add_column("Source", style="magenta")
        
        for p in projs:
            source = p.config.get("source_type", "N/A")
            table.add_row(str(p.id), p.name, source)
        console.print(table)

@app.command()
def branch(project_id: int, name: str):
    """Crée une nouvelle branche pour un projet."""
    with Session(engine) as session:
        new_branch = Branch(name=name, project_id=project_id)
        session.add(new_branch)
        session.commit()
        console.print(f"[bold green]✅ Branche '{name}' créée pour le projet {project_id}[/bold green]")

# ==============================================================================
# 2. SYNCHRONISATION (COMMIT) - VERSION OPTIMISÉE
# ==============================================================================

@app.command()
def sync(
    project_id: int = typer.Option(..., "--project", "-p"),
    branch_id: int = typer.Option(..., "--branch", "-b")
):
    """Capture l'état actuel (Appel direct au moteur de synchro)."""
    console.print(f"[bold yellow]🔄 Synchro en cours (Projet: {project_id}, Branche: {branch_id})...[/bold yellow]")
    
    try:
        # Appel direct de la fonction importée (plus robuste que subprocess)
        execute_sync(project_id, branch_id)
        console.print("[bold green]✨ Snapshot terminé, hashé et injecté dans Neo4j ![/bold green]")
    except Exception as e:
        console.print(f"[bold red]❌ Erreur lors de la synchro : {e}[/bold red]")
        import traceback
        logger.error(traceback.format_exc())

# ==============================================================================
# 3. INSPECTION (STATUS & DIFF)
# ==============================================================================

@app.command()
def status(project_id: Optional[int] = None):
    """Affiche l'historique des snapshots."""
    with Session(engine) as session:
        statement = select(Snapshot)
        if project_id:
            statement = statement.where(Snapshot.project_id == project_id)
        
        snaps = session.exec(statement.order_by(Snapshot.id.desc()).limit(10)).all()
        
        table = Table(title="📜 Historique des Snapshots (Commits)")
        table.add_column("ID", style="cyan")
        table.add_column("Branche", style="blue")
        table.add_column("Objets", style="yellow")
        table.add_column("Statut", style="green")

        for s in snaps:
            table.add_row(str(s.id), str(s.branch_id), str(s.total_objects), s.status)
        console.print(table)

@app.command()
def diff(base: int, target: int, project_id: int):
    """Compare deux snapshots (PR Style)."""
    console.print(Panel(f"🔍 Comparaison Snap #{base} ➔ Snap #{target}", style="bold blue"))
    
    try:
        diff_engine = DiffEngine(base, target, project_id=project_id)
        report = diff_engine.generate_detailed_report()
        
        # Résumé
        console.print(
            f"[green]+ {report['summary']['created']} Créés[/green] | "
            f"[blue]~ {report['summary']['updated']} Modifiés[/blue] | "
            f"[red]- {report['summary']['deleted']} Supprimés[/red]"
        )

        # ⚡ SECTION 1 : OBJETS CRÉÉS
        if report["details"]["created"]:
            console.print("\n[bold green]➕ Objets créés :[/bold green]")
            for item in report["details"]["created"]:
                console.print(f"   • {item['type']} #{item['id']}")

        # ⚡ SECTION 2 : OBJETS SUPPRIMÉS (C'ÉTAIT LE PROBLÈME !)
        if report["details"]["deleted"]:
            console.print("\n[bold red]➖ Objets supprimés :[/bold red]")
            for item in report["details"]["deleted"]:
                console.print(f"   • {item['type']} #{item['id']}")
                
                # Affichage des relations perdues
                if item.get("lost_relations"):
                    console.print(f"     [dim]Relations perdues : {', '.join(item['lost_relations'])}[/dim]")

        # SECTION 3 : OBJETS MODIFIÉS
        if report["details"]["updated"]:
            console.print("\n[bold blue]🔎 Détails des modifications :[/bold blue]")

            for item in report["details"]["updated"]:
                obj_type = item["type"]
                obj_id = item["id"]

                console.print(f"\n📝 [bold cyan]{obj_type} #{obj_id}[/bold cyan]")

                # On récupère le diff déjà calculé par le moteur
                diff_data = item.get("diff", {})

                # 1. Affichage des PROPRIÉTÉS
                props = diff_data.get("properties", {})
                if props:
                    for field, val in props.items():
                        console.print(
                            f"   • {field}: [red]{val['old']}[/red] ➔ [green]{val['new']}[/green]"
                        )

                # 2. Affichage des RELATIONS
                rels = diff_data.get("relations", {})
                
                if rels.get("removed"):
                    for removed in rels["removed"]:
                        console.print(f"   [bold red]🔗 Relation supprimée : {removed}[/bold red]")
                
                if rels.get("added"):
                    for added in rels["added"]:
                        console.print(f"   [bold green]🔗 Relation ajoutée : {added}[/bold green]")

                # Si aucun changement détecté
                if not props and not rels.get("removed") and not rels.get("added"):
                    console.print("   [dim]Aucun changement détecté[/dim]")

    except Exception as e:
        console.print(f"[bold red]❌ Erreur Diff : {e}[/bold red]")
        import traceback
        logger.error(traceback.format_exc())
        
# ==============================================================================
# 4. RESTAURATION AVEC PRE-FLIGHT (STARSHIP MODE)
# ==============================================================================

@app.command()
def restore(
    snap_id: int, 
    project_id: int,
    dry_run: bool = typer.Option(False, "--dry-run"),
    force: bool = typer.Option(False, "--force", "-f")
):
    """Restaure un snapshot avec analyse d'impact et batching."""
    console.print(Panel(f"🛠️  Restauration du Snap #{snap_id}", style="bold magenta"))
    
    try:
        from src.connectors.factory import ConnectorFactory
        
        with Session(engine) as session:
            project = session.get(SnapshotProject, project_id)
            connector = ConnectorFactory.get_connector(
                project.config.get("source_type", "hubspot"), 
                project.config, 
                project_id=project.id,
                project_config=project.config
            )
            
            # 1. Initialisation du moteur (Batch + Translation d'ID)
            re = RestoreEngine(project_id, connector, snapshot_id=snap_id, dry_run=dry_run)
            
            # 2. ANALYSE PRE-FLIGHT (Impact)
            with console.status("[bold green]Analyse des différences Snap ↔ CRM..."):
                analysis = re.get_preflight_report()
            
            # 3. Affichage du rapport
            re.display_preflight(analysis)

            # 4. Confirmation utilisateur
            if not dry_run and not force:
                if not Confirm.ask("\n🔥 [bold red]Confirmez-vous l'application de ces changements sur le CRM ?[/]"):
                    console.print("[yellow]Opération annulée par l'utilisateur.[/yellow]")
                    return

            # 5. Exécution (Batching & Suture automatique)
            if dry_run:
                console.print("\n[yellow]🧪 MODE SIMULATION ACTIVÉ[/yellow]")
            
            report = re.run()
            
            console.print(f"\n[bold green]🏁 Opération terminée ![/bold green]")
            console.print(f"✅ Succès : {report['success']} | ❌ Échecs : {report['failed']} | 😴 Ignorés : {report.get('ignored', 0)}")
            
    except Exception as e:
        console.print(f"[bold red]❌ Erreur Restore : {e}[/bold red]")
        import traceback
        logger.error(traceback.format_exc())

# ==============================================================================
# 5. GRAPHE & SANTÉ
# ==============================================================================

@app.command()
def graph_sync(snapshot_id: int, project_id: int):
    """Génère le graphe Neo4j (Suture manuelle)."""
    from src.core.graph import GraphManager
    from src.core.snapshot import SnapshotEngine
    
    graph_mgr = GraphManager()
    snap_engine = SnapshotEngine(snapshot_id=snapshot_id)
    object_types = ["companies", "contacts", "deals"] 

    for obj_type in object_types:
        items = snap_engine.get_all_items_from_minio(obj_type)
        links_batch = []
        for item in items:
            from_id = str(item.get("id"))
            rels = item.get("_zibridge_links", {})
            for target_type, ids in rels.items():
                for t_id in ids:
                    links_batch.append({"from_id": from_id, "to_id": str(t_id), "to_type": target_type})
        
        if links_batch:
            graph_mgr.link_entities_batch(project_id, obj_type, links_batch)
            console.print(f"🕸️ Graphe : {len(links_batch)} liens créés pour {obj_type}")

@app.command()
def health_check(project_id: int):
    """Vérifie la santé des données via Neo4j."""
    from src.core.graph import GraphManager
    gm = GraphManager()
    console.print(f"[bold blue]🩺 Analyse de santé pour le projet {project_id}...[/bold blue]")
    
    orphans = gm.get_orphan_entities(project_id, "contacts", "companies")
    if orphans:
        console.print(f"[bold red]❌ {len(orphans)} Contacts Orphelins détectés ![/bold red]")
        for o_id in orphans[:10]: console.print(f"  - Contact ID: {o_id}")
    else:
        console.print("[bold green]✅ Aucun contact orphelin. Toutes les sutures sont OK.[/bold green]")

if __name__ == "__main__":
    app()