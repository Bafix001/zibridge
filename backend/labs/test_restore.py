from src.core.restore import RestoreEngine
from loguru import logger

def manual_rollback(snap_id: int):
    logger.info(f"🛠️ Démarrage du test de restauration pour le Snap #{snap_id}")
    
    # Initialisation du moteur
    engine = RestoreEngine(snapshot_id=snap_id)
    
    # Exécution
    # On cible uniquement les contacts pour ce test rapide
    report = engine.run_full_restore(object_types=["contacts"])
    
    print("\n" + "="*30)
    print(f"RAPPORT DE RESTAURATION SNAP #{snap_id}")
    print(f"✅ Succès : {report['success']}")
    print(f"❌ Échecs : {report['failed']}")
    print("="*30)

if __name__ == "__main__":
    # Remplace 19 par l'ID de ton snapshot "propre"
    target_snap = 19 
    manual_rollback(target_snap)