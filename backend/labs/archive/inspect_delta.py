import os
import json
from boto3 import Session
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# --- CONFIGURATION MINIO ---
endpoint = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
if not endpoint.startswith('http'):
    endpoint = f"http://{endpoint}"

s3_client = Session().client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    region_name="us-east-1"
)
BUCKET = os.getenv("MINIO_BUCKET", "zibridge")

def get_json_from_minio(path):
    obj = s3_client.get_object(Bucket=BUCKET, Key=path)
    return json.loads(obj['Body'].read().decode('utf-8'))

def compare_snaps(old_id, new_id, obj_type, obj_id):
    try:
        # Construction des chemins (Vérifie si tes fichiers sont bien dans 'snapshots/')
        old_path = f"snapshots/{old_id}/{obj_type}/{obj_id}.json"
        new_path = f"snapshots/{new_id}/{obj_type}/{obj_id}.json"
        
        old_data = get_json_from_minio(old_path).get('properties', {})
        new_data = get_json_from_minio(new_path).get('properties', {})

        diffs = []
        for key in set(old_data.keys()) | set(new_data.keys()):
            v1, v2 = old_data.get(key), new_data.get(key)
            if v1 != v2:
                diffs.append(f"   🔶 [{key}]: {v1} ➔ {v2}")
        
        if diffs:
            print(f"\n✨ Changements détectés pour {obj_type} #{obj_id} :")
            for d in diffs: print(d)
            return True
    except Exception:
        return False
    return False

def run_inspection(old_snap, new_snap):
    logger.info(f"🕵️ Analyse des différences entre Snap {old_snap} et {new_snap}...")
    
    # On scanne les types d'objets
    for obj_type in ["companies", "contacts"]:
        prefix = f"snapshots/{new_snap}/{obj_type}/"
        try:
            response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            if 'Contents' not in response:
                continue
                
            for obj in response['Contents']:
                # On extrait l'ID (ex: snapshots/8/companies/123.json -> 123)
                obj_id = obj['Key'].split('/')[-1].replace('.json', '')
                compare_snaps(old_snap, new_snap, obj_type, obj_id)
        except Exception as e:
            logger.error(f"Erreur lors du scan {obj_type}: {e}")

if __name__ == "__main__":
    run_inspection(7, 8)