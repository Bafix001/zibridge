import pandas as pd
import time
import os
from src.connectors.rest_api import RestApiConnector
from loguru import logger

def seed_hubspot():
    connector = RestApiConnector()
    base_path = "dataset/saas-dataset"
    
    # 1. Injection des Companies
    if os.path.exists(f"{base_path}/companies.csv"):
        logger.info("🏢 Lecture des entreprises...")
        df_co = pd.read_csv(f"{base_path}/companies.csv")
        for _, row in df_co.head(20).iterrows(): # On commence par 20 pour tester
            payload = {
                "name": row['name'],
                "domain": row['domain'],
                "industry": row['industry']
            }
            connector.create_object("companies", payload)
            time.sleep(0.2) # Sécurité pour le rate-limit

    # 2. Injection des Contacts
    if os.path.exists(f"{base_path}/contacts.csv"):
        logger.info("👤 Lecture des contacts...")
        df_ct = pd.read_csv(f"{base_path}/contacts.csv")
        for _, row in df_ct.head(50).iterrows(): # On envoie les 50 premiers
            payload = {
                "email": row['email'],
                "firstname": row['firstname'],
                "lastname": row['lastname'],
                "jobtitle": row['jobtitle']
            }
            connector.create_object("contacts", payload)
            time.sleep(0.2)

    logger.success("🚀 Injection initiale terminée !")

if __name__ == "__main__":
    seed_hubspot()