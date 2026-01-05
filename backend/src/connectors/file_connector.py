import pandas as pd
import os
import hashlib
import tempfile
import shutil
from loguru import logger
from typing import Dict, List, Any, Optional
from collections import defaultdict

class FileConnector:
    """
    Connecteur UNIVERSEL pour fichiers tabulaires.
    Supporte : CSV, Excel (.xlsx, .xls, .xlsm), TSV, TXT, ODS
    Architecture hybride : source_type="file" + détection automatique du format
    """
    
    # 🔥 Formats supportés avec détection automatique
    SUPPORTED_FORMATS = {
        '.csv': 'csv',
        '.tsv': 'tsv',
        '.txt': 'txt',
        '.xlsx': 'excel',
        '.xls': 'excel',
        '.xlsm': 'excel',
        '.ods': 'ods',
    }
    
    def __init__(self, credentials):
        self.file_path = credentials.get("file_path")
        self.file_format = credentials.get("file_format", "auto")  # 🔥 Hybride : auto ou forcé
        self.df = None
        self.entity_types: Dict[str, str] = {}
        
        # 🔥 Cache pour performances
        self._id_cache: Dict[str, str] = {}
        self._entity_index: Dict[str, Dict[str, int]] = defaultdict(dict)
        
        # ✅ Dictionnaire étendu pour l'agnosticisme TOTAL
        self.ALIASES = {
            "company": ["company", "entreprise", "societe", "org", "organization", "client", "account", "business", "vendor"],
            "contact": ["contact", "client", "utilisateur", "personne", "user", "person", "lead", "employee", "customer"],
            "deal": ["deal", "affaire", "opportunite", "sale", "transaction", "order", "contrat", "opportunity"],
            "ticket": ["ticket", "incident", "requete", "support", "task", "issue", "bug", "case"],
            "product": ["product", "produit", "article", "item", "service", "sku"]
        }

        if self.file_path and os.path.exists(self.file_path):
            try:
                self._load_file()
                logger.info(f"✅ Fichier chargé : {self.file_path} ({len(self.df)} lignes, format: {self.file_format})")
                self._auto_classify_columns()
                self._build_indexes()
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du fichier : {e}")

    def _detect_format(self) -> str:
        """
        🔍 Détecte automatiquement le format via l'extension.
        Peut être overridé par credentials['file_format']
        """
        _, ext = os.path.splitext(self.file_path)
        ext_lower = ext.lower()
        
        if ext_lower in self.SUPPORTED_FORMATS:
            detected = self.SUPPORTED_FORMATS[ext_lower]
            logger.info(f"📂 Format détecté : {detected.upper()} (extension: {ext_lower})")
            return detected
        
        logger.warning(f"⚠️  Extension inconnue '{ext}', fallback sur CSV")
        return 'csv'

    def _load_file(self):
        """
        🔥 CHARGEMENT UNIVERSEL : Détecte et charge n'importe quel format
        Architecture hybride : auto-detect OU format forcé
        """
        # 🔥 Si format = "auto", détecte automatiquement
        if self.file_format == "auto":
            self.file_format = self._detect_format()
        
        logger.info(f"📥 Chargement du fichier en mode {self.file_format.upper()}...")
        
        # 🔥 Chargement selon le format
        if self.file_format == 'csv':
            # Essaie différents délimiteurs
            try:
                self.df = pd.read_csv(self.file_path, dtype=str)
            except:
                # CSV européen avec point-virgule
                logger.info("🔄 Tentative avec délimiteur ';'")
                self.df = pd.read_csv(self.file_path, dtype=str, sep=';')
        
        elif self.file_format == 'tsv' or self.file_format == 'txt':
            self.df = pd.read_csv(self.file_path, dtype=str, sep='\t')
        
        elif self.file_format == 'excel':
            # 🔥 Gère les fichiers Excel multi-feuilles
            excel_file = pd.ExcelFile(self.file_path)
            
            if len(excel_file.sheet_names) > 1:
                logger.info(f"📊 {len(excel_file.sheet_names)} feuilles détectées: {excel_file.sheet_names}")
                sheet_name = excel_file.sheet_names[0]  # Prend la première
                logger.info(f"📄 Utilisation de la feuille : '{sheet_name}'")
            else:
                sheet_name = 0
            
            self.df = pd.read_excel(self.file_path, sheet_name=sheet_name, dtype=str)
        
        elif self.file_format == 'ods':
            # LibreOffice/OpenOffice
            self.df = pd.read_excel(self.file_path, engine='odf', dtype=str)
        
        else:
            raise ValueError(f"Format '{self.file_format}' non supporté")
        
        # Nettoyage standard
        self.df.columns = self.df.columns.str.strip()
        logger.success(f"✅ {len(self.df)} lignes chargées depuis {self.file_format.upper()}")

    def _auto_classify_columns(self):
        """🔥 DÉTECTE TOUS LES TYPES AUTOMATIQUEMENT"""
        if self.df is None:
            return
        
        self.entity_types = {}
        for col in self.df.columns:
            col_lower = col.lower()
            detected = False
            
            for entity_type, patterns in self.ALIASES.items():
                if any(pattern in col_lower for pattern in patterns):
                    self.entity_types[col] = entity_type
                    detected = True
                    break
            
            if not detected:
                self.entity_types[col] = f"custom_{len(self.entity_types)}"
            
        logger.info(f"🔍 AUTO-DÉTECTÉ {len(self.entity_types)} types: {dict(list(self.entity_types.items())[:5])}...")

    def _build_indexes(self):
        """🚀 OPTIMISATION : Pré-calcule tous les IDs et index"""
        if self.df is None:
            return
        
        logger.info("🔨 Construction des index...")
        
        for idx, row in self.df.iterrows():
            for entity_type in set(self.entity_types.values()):
                entity_cols = [col for col, etype in self.entity_types.items() if etype == entity_type]
                if not entity_cols:
                    continue
                
                row_id = self._generate_id(row, entity_cols)
                self._entity_index[entity_type][row_id] = idx
        
        logger.success(f"✅ Index construits : {sum(len(v) for v in self._entity_index.values())} entrées")

    def _generate_id(self, row: pd.Series, entity_cols: List[str]) -> str:
        """
        🔥 Génère un ID STABLE basé sur une clé primaire naturelle.
        Priorité : email > id > première colonne non vide
        """
        # 1. 🔥 CHERCHE UNE CLÉ PRIMAIRE NATURELLE (par ordre de priorité)
        primary_key_patterns = [
            "email", "mail", "e-mail",           # Email (très stable)
            "id", "_id", "objectid", "object_id", # ID explicite
            "name", "nom",                        # Nom (assez stable)
            "phone", "telephone", "tel"           # Téléphone
        ]
        
        primary_key_value = None
        
        # Cherche dans les colonnes de cet entity type
        for pattern in primary_key_patterns:
            for col in entity_cols:
                if pattern in col.lower():
                    val = str(row.get(col, "")).strip()
                    if val and val.lower() not in ["nan", "none", "", "null"]:
                        primary_key_value = val
                        logger.debug(f"🔑 Clé primaire trouvée : {col} = {val[:20]}")
                        break
            if primary_key_value:
                break
        
        # 2. 🔥 FALLBACK : Première valeur non vide dans entity_cols
        if not primary_key_value:
            for col in entity_cols:
                val = str(row.get(col, "")).strip()
                if val and val.lower() not in ["nan", "none", "", "null"]:
                    primary_key_value = val
                    logger.debug(f"⚠️  Clé par défaut : {col} = {val[:20]}")
                    break
        
        # 3. 🔥 DERNIER RECOURS : Hash de TOUTES les valeurs (trié pour stabilité)
        if not primary_key_value:
            # Trie les colonnes pour garantir l'ordre
            sorted_values = [str(row.get(col, "")) for col in sorted(entity_cols)]
            primary_key_value = "".join(sorted_values)
            logger.warning(f"⚠️  Aucune clé primaire - Hash de toutes les valeurs")
        
        # 4. Génère l'ID stable
        if primary_key_value not in self._id_cache:
            # 🔥 Préfixe avec le type d'entité pour éviter collisions entre types
            entity_type = self.entity_types.get(entity_cols[0], "unknown")
            hash_base = f"{entity_type}:{primary_key_value}"
            
            self._id_cache[primary_key_value] = f"file_{hashlib.md5(hash_base.encode()).hexdigest()[:16]}"
        
        return self._id_cache[primary_key_value]
    
    def get_detected_entities(self) -> Dict[str, str]:
        """Retourne tous les types détectés."""
        return self.entity_types

    def extract_entities(self, entity_type: str) -> List[Dict]:
        """✅ Extrait TOUS les objets d'un type donné (OPTIMISÉ)"""
        if self.df is None:
            return []

        entity_cols = [col for col, etype in self.entity_types.items() if etype == entity_type]
        if not entity_cols:
            logger.warning(f"⚠️ Aucune colonne pour '{entity_type}'")
            return []

        results = []
        for idx, row in self.df.iterrows():
            item = {col: row[col] for col in entity_cols if col in row}
            item["id"] = self._generate_id(row, entity_cols)
            item["_zibridge_links"] = self._auto_suture_links_optimized(row, entity_cols, entity_type)
            results.append(self.normalize_data(item, entity_type))
        
        logger.success(f"✅ {entity_type} synchronisés : {len(results)}")
        return results

    def _auto_suture_links_optimized(self, row: pd.Series, my_cols: List[str], my_entity: str) -> Dict[str, List[str]]:
        """🚀 SUTURE INTELLIGENTE OPTIMISÉE"""
        links = defaultdict(list)
        
        for col in my_cols:
            col_val = str(row[col] or "").strip().lower()
            if not col_val:
                continue
            
            for other_entity, entity_index in self._entity_index.items():
                if other_entity == my_entity:
                    continue
                
                other_cols = [c for c, e in self.entity_types.items() if e == other_entity]
                
                for other_col in other_cols:
                    other_val = str(row.get(other_col, "")).strip().lower()
                    if not other_val:
                        continue
                    
                    if col_val in other_val or other_val in col_val:
                        link_id = self._generate_id(row, other_cols)
                        links[f"{other_entity}s"].append(link_id)
        
        return dict(links)

    def count_entities(self, entity_type: str) -> int:
        """Compte les objets pour un type."""
        if entity_type in self._entity_index:
            return len(self._entity_index[entity_type])
        return 0

    def normalize_data(self, data: dict, object_type: str) -> dict:
        """Nettoie les données pour éviter les erreurs SQL/Graph."""
        normalized = {}
        for key, value in data.items():
            if key.startswith('_'):
                normalized[key] = data[key]
                continue
            if pd.isna(value) or str(value).lower() in ['nan', 'none', '']:
                normalized[key] = None
            else:
                normalized[key] = str(value)
        return normalized

    def extract_data(self, obj_type: str):
        """Alias vers la nouvelle méthode."""
        return self.extract_entities(obj_type.rstrip('s'))
    
    def get_entity_counts(self) -> Dict[str, int]:
        """📊 Retourne le décompte des entités détectées."""
        counts = {"companies": 0, "contacts": 0, "deals": 0, "tickets": 0, "products": 0}
        
        if self.df is None:
            return counts
        
        present_types = set(self.entity_types.values())
        for etype in present_types:
            plural = f"{etype}s"
            if plural in counts:
                counts[plural] = len(self.df)
            elif etype.startswith("custom_"):
                counts[etype] = len(self.df)
        
        return counts

    def push_update(self, object_type: str, object_id: str, properties: Dict[str, Any]) -> tuple[str, Optional[str]]:
        """
        🔥 ÉCRITURE TRANSACTIONNELLE avec fichier temporaire
        Sauvegarde dans le format d'origine (CSV, Excel, etc.)
        """
        if self.df is None:
            return ("failed", None)
        
        if object_id not in self._entity_index.get(object_type, {}):
            logger.warning(f"⚠️  ID {object_id} non trouvé pour {object_type}")
            return ("failed", None)
        
        row_idx = self._entity_index[object_type][object_id]
        
        modified = False
        for prop, value in properties.items():
            if prop in self.df.columns:
                self.df.at[row_idx, prop] = value
                modified = True
        
        if not modified:
            return ("failed", None)
        
        try:
            temp_dir = os.path.dirname(self.file_path)
            _, ext = os.path.splitext(self.file_path)
            temp_fd, temp_path = tempfile.mkstemp(suffix=ext, dir=temp_dir, text=True)
            os.close(temp_fd)
            
            # 🔥 Sauvegarde selon le format d'origine
            if self.file_format == 'excel':
                self.df.to_excel(temp_path, index=False)
            elif self.file_format == 'tsv':
                self.df.to_csv(temp_path, index=False, sep='\t')
            else:  # CSV par défaut
                self.df.to_csv(temp_path, index=False)
            
            shutil.move(temp_path, self.file_path)
            logger.success(f"💾 Fichier {self.file_path} mis à jour (ID: {object_id})")
            return ("updated", None)
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'écriture : {e}")
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
            return ("failed", None)

    @property
    def source_type(self) -> str:
        """🔥 Propriété agnostique : toujours 'file'"""
        return "file"
    
    def test_connection(self) -> bool:
        """Vérifie que le fichier est accessible."""
        return self.file_path and os.path.exists(self.file_path) and self.df is not None
    
    def get_association_definition(self, from_type: str, to_type: str) -> Any:
        """Les fichiers n'ont pas besoin de définitions d'associations."""
        return None
    
    def create_association(self, from_type: str, from_id: str, to_type: str, to_id: str, assoc_type_id: Any) -> bool:
        """Les fichiers gèrent les associations via _zibridge_links."""
        return True