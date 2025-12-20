import typer
import subprocess
import sys
from loguru import logger
from rich.console import Console
from rich.table import Table
from sqlmodel import Session, select, func
import warnings

# Imports internes
from src.core.diff import DiffEngine
from src.core.restore import RestoreEngine
from src.utils.db import engine, storage_manager
from src.core.models import Snapshot, SnapshotItem

# Suppression des warnings SSL polluants sur Mac
warnings.filterwarnings("ignore", message=".*OpenSSL 1.1.1+.*")

app = typer.Typer(help="🚀 Zibridge CLI - Système de Versioning pour CRM")
console = Console()

@app.command()
def sync():
    """Capture l'état actuel du CRM et crée un nouveau Snapshot."""
    console.print("[bold green]🔄 Lancement de la synchronisation globale...[/bold green]")
    try:
        import os
        # On ajoute le dossier actuel au PYTHONPATH pour que 'src' soit trouvé
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd() 

        subprocess.run(
            [sys.executable, "scripts/run_sync.py"], 
            check=True, 
            env=env  # On passe l'environnement mis à jour
        )
        console.print("[bold green]✨ Synchronisation terminée avec succès ![/bold green]")
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]❌ Erreur lors de la synchronisation : {e}[/bold red]")

@app.command()
def status():
    """Affiche la liste des Snapshots avec le nombre d'objets contenus."""
    with Session(engine) as session:
        statement = select(Snapshot).order_by(Snapshot.id.desc()).limit(15)
        snaps = session.exec(statement).all()
        
        if not snaps:
            console.print("[yellow]⚠️ Aucun snapshot trouvé. Lancez 'python zibridge.py sync'.[/yellow]")
            return

        table = Table(title="📜 Historique des Snapshots Zibridge")
        table.add_column("ID", style="cyan", justify="center")
        table.add_column("Date de création", style="magenta")
        table.add_column("Objets", style="yellow", justify="right")
        table.add_column("Source", style="green")

        for s in snaps:
            # Comptage temps réel des items liés
            count_stmt = select(func.count()).select_from(SnapshotItem).where(SnapshotItem.snapshot_id == s.id)
            count = session.exec(count_stmt).one()
            
            date_val = getattr(s, 'created_at', None) or getattr(s, 'timestamp', "N/A")
            date_str = date_val.strftime("%Y-%m-%d %H:%M") if hasattr(date_val, 'strftime') else str(date_val)
            
            table.add_row(str(s.id), date_str, str(count), s.source or "HubSpot API")
        
        console.print(table)

@app.command()
def diff(base: int, target: int):
    """Compare deux Snapshots et affiche les changements détaillés (CAS-based)."""
    console.print(f"[bold]🤖 Analyse du Delta entre Snap #{base} et Snap #{target}...[/bold]")
    
    try:
        diff_engine = DiffEngine(base, target)
        report = diff_engine.generate_report()
        
        # 1. Résumé statistique
        console.print(f"\n[bold yellow]📊 BILAN :[/bold yellow]")
        console.print(f"  [green]+ {len(report['created'])} Créations[/green]")
        console.print(f"  [blue]~ {len(report['updated'])} Modifications[/blue]")
        console.print(f"  [red]- {len(report['deleted'])} Suppressions[/red]\n")
        
        # 2. Affichage des créations
        if report['created']:
            console.print("[bold green]🆕 Nouveaux objets :[/bold green]")
            for item in report['created']:
                console.print(f"   [green]✔[/green] {item['type']} #{item['id']}")

        # 3. Affichage des modifications détaillées
        if report['updated']:
            console.print("\n[bold blue]📝 Détail des modifications :[/bold blue]")
            for item in report['updated']:
                # Récupération des deux versions du JSON dans MinIO
                old_json = storage_manager.get_json(f"blobs/{item['old_hash']}.json")
                new_json = storage_manager.get_json(f"blobs/{item['new_hash']}.json")
                
                p1 = old_json.get('properties', old_json)
                p2 = new_json.get('properties', new_json)
                
                console.print(f"\n📦 [cyan]{item['type']} #{item['id']}[/cyan]")
                
                # Comparaison clé par clé des propriétés
                for key in sorted(set(p1.keys()) | set(p2.keys())):
                    val1, val2 = p1.get(key), p2.get(key)
                    if val1 != val2:
                        console.print(f"   🔶 {key}: [red]{val1}[/red] ➔ [green]{val2}[/green]")

        # 4. Affichage des suppressions
        if report['deleted']:
            console.print("\n[bold red]🗑️ Objets disparus (présents dans #{base} mais pas dans #{target}) :[/bold red]")
            for item in report['deleted']:
                console.print(f"   [red]✘[/red] {item['type']} #{item['id']}")

    except Exception as e:
        console.print(f"[bold red]❌ Erreur lors du calcul du diff : {e}[/bold red]")

@app.command()
def restore(
    snap_id: int, 
    only: str = typer.Option(None, "--only", "-o", help="Cibler un objet (ex: 'companies/123')")
):
    """Restaure les données du CRM vers un état passé (méthode classique)."""
    target_msg = f"le Snap #{snap_id}" if not only else f"l'objet {only}"
    
    if not typer.confirm(f"⚠️ Êtes-vous sûr de vouloir écraser les données actuelles par {target_msg} ?"):
        raise typer.Abort()

    console.print(f"[bold blue]🛠️ Restauration en cours...[/bold blue]")
    try:
        restore_engine = RestoreEngine(snapshot_id=snap_id)
        report = restore_engine.run_full_restore(
            object_types=["companies", "contacts", "deals"],
            target_only=only
        )
        console.print(f"\n[bold green]🏁 Rollback terminé ![/bold green]")
        console.print(f"✅ Succès : {report['success']} | ❌ Échecs : {report['failed']}")
    except Exception as e:
        console.print(f"[bold red]❌ Erreur critique de restauration : {e}[/bold red]")

@app.command()
def smart_restore(
    snap_id: int,
    selective: bool = typer.Option(True, "--selective/--full", help="Mode sélectif (uniquement changements) ou complet"),
    skip_checks: bool = typer.Option(False, "--skip-checks", help="Ignorer les vérifications de cohérence")
):
    """
    🧠 Restauration intelligente avec Auto-Suture des associations.
    
    Par défaut, restaure UNIQUEMENT les objets modifiés/supprimés (mode sélectif).
    Utiliser --full pour restaurer TOUT le snapshot.
    
    Features:
    - Mode sélectif : Restaure uniquement les changements (défaut)
    - Ordre de dépendances respecté (companies → contacts → deals)
    - Mapping automatique des IDs (old → new)
    - Auto-Suture des associations
    - Analyse d'impact relationnelle
    
    Exemples:
    - Restauration sélective (défaut): python zibridge.py smart-restore 10
    - Restauration complète: python zibridge.py smart-restore 10 --full
    - Sans vérifications: python zibridge.py smart-restore 10 --skip-checks
    """
    
    mode = "SÉLECTIVE (uniquement changements)" if selective else "COMPLÈTE (tout le snapshot)"
    
    if not typer.confirm(f"⚠️ Restauration {mode} du Snapshot #{snap_id}. Continuer ?"):
        raise typer.Abort()

    console.print(f"[bold magenta]🧠 Restauration Intelligente en cours...[/bold magenta]")
    console.print(f"[dim]Mode: {mode}[/dim]")
    console.print("[dim]Ordre: Companies → Contacts → Deals[/dim]\n")
    
    try:
        restore_engine = RestoreEngine(snapshot_id=snap_id)
        
        if selective:
            # Mode sélectif : restaure uniquement les changements
            report = restore_engine.run_smart_restore_selective(skip_checks=skip_checks)
        else:
            # Mode complet : restaure tout
            report = restore_engine.run_smart_restore(skip_checks=skip_checks)
        
        console.print(f"\n[bold green]🎉 Restauration Intelligente terminée ![/bold green]")
        
        if selective and 'skipped' in report:
            console.print(f"""
[bold]Résumé :[/bold]
✅ Succès : {report.get('success', 0)}
✨ Ressuscités : {report.get('resurrected', 0)}
🔀 Fusionnés : {report.get('merged', 0)}
😴 Ignorés (identiques) : {report.get('skipped', 0)}
⚠️ Alertes : {report.get('warnings', 0)}
❌ Échecs : {report.get('failed', 0)}
            """)
        else:
            console.print(f"""
[bold]Résumé :[/bold]
✅ Succès : {report.get('success', 0)}
✨ Ressuscités : {report.get('resurrected', 0)}
🔀 Fusionnés : {report.get('merged', 0)}
⚠️ Alertes : {report.get('warnings', 0)}
❌ Échecs : {report.get('failed', 0)}
            """)
        
    except Exception as e:
        console.print(f"[bold red]❌ Erreur critique : {e}[/bold red]")
        import traceback
        console.print(traceback.format_exc())

if __name__ == "__main__":
    app()