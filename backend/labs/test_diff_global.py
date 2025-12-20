from src.core.diff import DiffEngine
from loguru import logger

def run_global_audit():
    # On compare le tout premier (Snap 1) au dernier (Snap 16)
    # Note : Vérifie bien que ton premier Snapshot est bien le ID 1 
    # Sinon utilise le plus petit ID présent dans ton audit_item.py
    start_snap = 1
    end_snap = 16
    
    logger.info(f"🕰️ Remontée dans le temps : Comparaison du Snap {start_snap} vs Snap {end_snap}")
    
    diff = DiffEngine(start_snap, end_snap)
    report = diff.generate_report()
    
    print("\n" + "═"*50)
    print(f"📊 BILAN GLOBAL DE DÉRIVE DES DONNÉES")
    print(f"Depuis l'origine (Snap {start_snap}) jusqu'à maintenant")
    print("═"*50)
    print(f"✨ Total créations  : {len(report['created'])}")
    print(f"🔄 Total corrections : {len(report['updated'])}")
    print(f"🗑️ Total suppressions: {len(report['deleted'])}")
    print(f"😴 Objets intacts   : {report['unchanged_count']}")
    print("═"*50)

    if report['updated']:
        print("\n📝 Liste exhaustive des objets ayant divergé :")
        for item in report['updated']:
            print(f"  • {item}")

if __name__ == "__main__":
    run_global_audit()