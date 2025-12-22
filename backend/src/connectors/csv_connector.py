import pandas as pd
import os
import hashlib
from loguru import logger
from typing import Dict, List


class CSVConnector:
    def __init__(self, credentials):
        self.source_type = "file"
        self.file_path = credentials.get("file_path")
        self.df = None
        self.entity_types: Dict[str, str] = {}  # {'company_name': 'company', 'user_email': 'contact'}
        
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
                self.df = pd.read_csv(self.file_path, dtype=str)
                logger.info(f"✅ CSV chargé : {self.file_path} ({len(self.df)} lignes)")
                self._auto_classify_columns()  # ✅ AUTO-DÉTECTION
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du CSV : {e}")

    def _auto_classify_columns(self):
        """🔥 FONCTION CŒUR : DÉTECTE TOUS LES TYPES AUTOMATIQUEMENT"""
        if self.df is None:
            return
        
        self.entity_types = {}
        for col in self.df.columns:
            col_lower = col.lower()
            detected = False
            
            # Recherche dans TOUS les patterns
            for entity_type, patterns in self.ALIASES.items():
                if any(pattern in col_lower for pattern in patterns):
                    self.entity_types[col] = entity_type
                    detected = True
                    break
            
            # ✅ Fallback : custom type dynamique
            if not detected:
                self.entity_types[col] = f"custom_{len(self.entity_types)}"
            
        logger.info(f"🔍 AUTO-DÉTECTÉ {len(self.entity_types)} types: {dict(list(self.entity_types.items())[:5])}...")

    def get_detected_entities(self) -> Dict[str, str]:
        """Retourne tous les types détectés."""
        return self.entity_types

    def extract_entities(self, entity_type: str) -> List[Dict]:
        """✅ NOUVEAU : Extrait TOUS les objets d'un type donné."""
        if self.df is None:
            return []

        # Trouve TOUTES les colonnes de ce type
        entity_cols = [col for col, etype in self.entity_types.items() if etype == entity_type]
        if not entity_cols:
            logger.warning(f"⚠️ Aucune colonne pour '{entity_type}'")
            return []

        results = []
        for _, row in self.df.iterrows():
            # Construit l'objet avec TOUTES les colonnes du type
            item = {col.replace(f"{entity_type}_", ""): row[col] for col in entity_cols if col in row}
            
            # ID stable MD5
            row_key = str(row[entity_cols[0]]) if entity_cols else str(row.name)
            item["id"] = f"csv_{hashlib.md5(row_key.encode()).hexdigest()[:8]}"
            
            # ✅ LIENS AUTOMATIQUES entre tous les types
            item["_zibridge_links"] = self._auto_suture_links(row, entity_cols)
            
            results.append(self.normalize_data(item, entity_type))
        
        logger.success(f"✅ {entity_type} synchronisés : {len(results)}")
        return results

    def _auto_suture_links(self, row: pd.Series, my_cols: List[str]) -> Dict[str, List[str]]:
        """🔥 SUTURE INTELLIGENTE : détecte liens entre TOUS les types."""
        links = {}
        
        # Pour CHAQUE colonne de mon objet, cherche les correspondances
        for col in my_cols:
            col_val = str(row[col] or "")
            if not col_val:
                continue
                
            my_entity = self.entity_types[col]
            
            # Cherche les correspondances dans les AUTRES types
            for other_col, other_entity in self.entity_types.items():
                if other_entity != my_entity and other_col != col:
                    other_val = str(row[other_col] or "")
                    if col_val.lower() in other_val.lower() or other_val.lower() in col_val.lower():
                        link_id = f"csv_{hashlib.md5(other_val.encode()).hexdigest()[:8]}"
                        if other_entity not in links:
                            links[other_entity + "s"] = []
                        links[other_entity + "s"].append(link_id)
        
        return links

    def count_entities(self, entity_type: str) -> int:
        """Compte les objets pour un type."""
        entity_cols = [col for col, etype in self.entity_types.items() if etype == entity_type]
        return len(entity_cols) if entity_cols else 0

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

    # ✅ BACKWARD COMPATIBILITY (pour sync_engine existant)
    def extract_data(self, obj_type: str):
        """Alias vers la nouvelle méthode."""
        return self.extract_entities(obj_type.rstrip('s'))
