import csv
from flask import Flask, jsonify, request
import os

app = Flask(__name__)

DATA_FOLDER = 'saas-dataset'

# --- SIMULATION DE BASE DE DONNÉES EN MÉMOIRE ---
# On charge les données une seule fois au lancement pour pouvoir les modifier
db = {
    "contacts": [],
    "companies": [],
    "deals": [],
    "products": []
}

def init_db():
    for obj_type in db.keys():
        file_path = os.path.join(DATA_FOLDER, f"{obj_type}.csv")
        if os.path.exists(file_path):
            with open(file_path, mode='r', encoding='utf-8') as f:
                db[obj_type] = list(csv.DictReader(f))
            print(f"✅ {obj_type.capitalize()} chargés : {len(db[obj_type])}")

init_db()

# --- ROUTE 1 : LISTER TOUT (GET) ---
@app.route('/api/<object_type>', methods=['GET'])
def get_data(object_type):
    if object_type not in db:
        return jsonify({"status": "error", "message": "Objet inconnu"}), 404
    
    return jsonify({
        "items": db[object_type],
        "total": len(db[object_type])
    })

# --- ROUTE 2 : METTRE À JOUR UN OBJET (PUT) ---
# C'est cette route qui manquait pour le RESTORE !
@app.route('/api/<object_type>/<string:item_id>', methods=['PUT'])
def update_data(object_type, item_id):
    if object_type not in db:
        return jsonify({"status": "error"}), 404
    
    updated_item = request.json
    
    # On cherche l'objet dans notre "base de données" locale
    # On vérifie l'ID (certains CSV ont 'id', d'autres 'contact_id')
    found = False
    for i, item in enumerate(db[object_type]):
        # On compare l'ID (on check plusieurs clés possibles)
        current_id = item.get('id') or item.get(f"{object_type[:-1]}_id")
        
        if str(current_id) == str(item_id):
            db[object_type][i] = updated_item # ON ÉCRASE AVEC LA VÉRITÉ DE MINIO
            found = True
            break
            
    if found:
        print(f"✨ RESTORE : {object_type} #{item_id} mis à jour !")
        return jsonify({"status": "success"}), 200
    else:
        return jsonify({"status": "error", "message": "Item non trouvé"}), 404

if __name__ == '__main__':
    print("🚀 CRM Simulé Zibridge lancé avec support RESTORE !")
    app.run(port=5000, debug=True)