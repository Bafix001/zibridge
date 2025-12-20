from src.core.diff import DiffEngine
import json

def run_auto_diff(old_id, new_id):
    diff = DiffEngine(old_id, new_id)
    report = diff.generate_report()
    
    print(f"\n📊 RAPPORT AUTOMATIQUE (Snap {old_id} -> Snap {new_id})")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🆕 Créations  : {len(report['created'])}")
    print(f"🔄 Updates    : {len(report['updated'])}")
    print(f"🗑️ Supprimés  : {len(report['deleted'])}")
    print(f"😴 Identiques : {report['unchanged_count']}")
    
    if report['updated']:
        print(f"\nDétail des modifications : {report['updated']}")

if __name__ == "__main__":
    # Teste avec tes derniers Snapshots (ex: 11 et 12)
    run_auto_diff(11, 12)