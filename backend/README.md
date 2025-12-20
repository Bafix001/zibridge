# 🌉 Zibridge

**Le "Git pour les Données" – Système de Data Version Control (DVC) pour CRM & ERP.**

Zibridge est une solution d'ingénierie de données permettant de versionner, comparer et restaurer des états complexes de systèmes métier. Il transforme des bases de données volatiles en archives immuables, auditables et restaurables à la demande.

---

## 🎯 Vision & Cas d'usage

Zibridge construit la couche de contrôle de version manquante pour les données d'entreprise.

* **Rollback Chirurgical** : Restaurer une seule entité (ex: un contact spécifique) sans impacter le reste de la base de données.
* **Audit de Conformité** : Comparaison granulaire (Diff) entre deux points temporels pour tracer chaque changement (qui, quoi, quand).
* **Sécurisation de Migrations** : Création de points de restauration ("Save points") avant des opérations massives sur les données.

---

## 🏗️ Architecture "Triple-Engine"

Le projet repose sur une architecture hybride où chaque composant garantit l'intégrité et la scalabilité du système :



* **PostgreSQL (SQLModel)** : Orchestration des métadonnées et gestion des versions via hachage **SHA-256**.
* **MinIO (S3 Object Storage)** : Stockage immuable des objets JSON bruts via une approche **Content-Addressable Storage**.
* **Neo4j** : Modélisation de la topologie et des relations entre entités pour l'analyse de lignage (Data Lineage).

---

## 🛠️ Stack Technologique

- **Langage** : Python 3.12
- **Interface** : Typer (CLI Framework)
- **Data** : SQLModel (ORM), Pydantic v2
- **Infrastructure** : PostgreSQL, Neo4j, MinIO, Redis
- **Monitoring** : Loguru (Advanced Logging)

---

## 🚀 Utilisation de la CLI

Zibridge se pilote entièrement via une interface en ligne de commande unifiée.

```bash
# 1. Capturer l'état actuel du CRM (Snapshot)
python zibridge.py sync

# 2. Lister l'historique des snapshots et leur statut
python zibridge.py status

# 3. Comparer deux points temporels (Audit Intelligent)
python zibridge.py diff 19 21

# 4. Restaurer tout un snapshot vers l'API CRM
python zibridge.py restore 19

# 5. Restauration chirurgicale (Data Surgery)
python zibridge.py restore 19 --only contacts/23


🚦 Démarrage Rapide

# Configuration de l'environnement
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Lancer l'infrastructure Docker (DBs & Stockage)
docker-compose up -d

# Vérifier l'état du système
python zibridge.py status


📁 Structure du Projet
Plaintext

zibridge/
├── src/
│   ├── core/         # Moteurs de Diff, Restore et Snapshot
│   ├── connectors/   # Connecteurs API REST (RestApiConnector)
│   ├── models/       # Modèles de données SQLModel
│   └── utils/        # Connecteurs DB, S3 et Graphe
├── labs/             # Scripts d'expérimentation et d'audit
├── docker/           # Configurations des services (MinIO, Neo4j, Postgres)
└── zibridge.py       # Point d'entrée unique de la CLI

📝 License

MIT License - voir LICENSE

Projet développé avec une approche Data-First pour garantir la résilience des systèmes métier.