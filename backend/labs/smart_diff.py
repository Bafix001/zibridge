from sqlmodel import Session, select, func
from src.utils.db import engine
from src.core.models import Snapshot, SnapshotItem
from src.core.diff import DiffEngine
from loguru import logger

def run_smart_audit():
    with Session(engine) as session:
        # 1. On cherche le premier Snapshot "Complet" (Baseline)
        # On cible le premier snap qui contient au moins 900 items
        first_snap_id = session.exec(
            select(SnapshotItem.snapshot_id)
            .group_by(SnapshotItem.snapshot_id)
            .having(func.count(SnapshotItem.id) >= 900)
            .order_by(SnapshotItem.snapshot_id.asc())
        ).first()

        # 2. On cherche le dernier Snapshot (Actuel)
        last_snap_id = session.exec(
            select(func.max(Snapshot.id))
        ).first()

        if not first_snap_id or not last_snap_id or first_snap_id == last_snap_id:
            logger.warning(f"⚠️ Pas assez de données pour comparer. Baseline: {first_snap_id}, Last: {last_snap_id}")
            return

        logger.info(f"🤖 Audit Intelligent : Comparaison Baseline (Snap {first_snap_id}) vs Actuel (Snap {last_snap_id})")
        
        # 3. Lancement du moteur de comparaison
        diff = DiffEngine(first_snap_id, last_snap_id)
        report = diff.generate_report()
        
        print("\n" + "═"*60)
        print(f"📊 RAPPORT D'ÉVOLUTION DU CRM (ZIBRIDGE)")
        print("═"*60)
        print(f"📍 Point de référence : Snapshot {first_snap_id} (Données complètes)")
        print(f"📍 État actuel        : Snapshot {last_snap_id}")
        print("-" * 60)
        print(f"🔄 MODIFICATIONS DÉTECTÉES : {len(report['updated'])}")
        print(f"🆕 NOUVEAUX ÉLÉMENTS      : {len(report['created'])}")
        print(f"🗑️ SUPPRESSIONS           : {len(report['deleted'])}")
        print(f"😴 OBJETS INTACTS         : {report['unchanged_count']}")
        print("═"*60)

        if report['updated']:
            print("\n📝 Détails des modifications :")
            for item in report['updated']:
                print(f"  • {item}")
        
        if report['created'] and len(report['created']) < 20:
            print("\n🆕 Détails des créations :")
            for item in report['created']:
                print(f"  • {item}")

if __name__ == "__main__":
    run_smart_audit()