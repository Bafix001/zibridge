from sqlmodel import Session, select
from src.utils.db import engine
from src.core.models import Snapshot
from src.core.diff import DiffEngine
from loguru import logger

def verify_restore(source_id: int, result_id: int):
    logger.info(f"🔍 Vérification : Snap {source_id} (Source) == Snap {result_id} (Résultat Restore)")
    
    diff = DiffEngine(source_id, result_id)
    report = diff.generate_report()
    
    print("\n" + "═"*60)
    print(f"🏁 RÉSULTAT DE LA VÉRIFICATION")
    print("═"*60)
    
    total_diffs = len(report['updated']) + len(report['created']) + len(report['deleted'])
    
    if total_diffs == 0:
        logger.success("🏆 PARFAIT : Le CRM est exactement dans l'état du Snapshot source !")
    else:
        logger.warning(f"⚠️ Il reste {total_diffs} différences.")
        print(f"🔄 Modifs    : {len(report['updated'])}")
        print(f"🆕 Créations : {len(report['created'])}")
        print(f"🗑️ Suppr.    : {len(report['deleted'])}")

if __name__ == "__main__":
    # On compare le Snap 19 (ce qu'on voulait) au Snap 21 (ce qu'on a obtenu)
    verify_restore(19, 21)