from src.connectors.mock import MockConnector
from loguru import logger

def run_test():
    # 1. On initialise notre faux connecteur
    connector = MockConnector()

    # 2. On vérifie la connexion
    if connector.test_connection():
        logger.success("Connexion simulée établie !")

    # 3. On lance l'extraction
    # Remarque : extract_data ne renvoie PAS une liste, mais un 'générateur'
    data_stream = connector.extract_data("contacts")

    logger.info("Début de la boucle de traitement...")
    
    # C'est ici que la magie opère : 
    # La boucle 'for' demande au générateur le prochain élément.
    # Le code dans MockConnector s'exécute jusqu'au prochain 'yield', puis s'arrête.
    for i, item in enumerate(data_stream):
        logger.info(f"🚀 Moteur : Reçu contact n°{i+1} -> {item['email']}")
        # Imagine qu'ici on calcule le HASH et qu'on enregistre dans MINIO
        logger.debug(f"💾 Moteur : Enregistrement de {item['id']} terminé.\n")

    logger.success("Traitement de tous les contacts terminé.")

if __name__ == "__main__":
    run_test()