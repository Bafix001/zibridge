from typing import Dict, List, Set
from loguru import logger
from src.utils.db import neo4j_driver


class GraphManager:
    def __init__(self):
        self.driver = neo4j_driver

    # ================== VERSIONNAGE ==================

    def update_relation(self, snapshot_id: int, object_type: str, external_id: str, item_hash: str):
        """Versionne l'entité dans le temps."""
        with self.driver.session() as session:
            query = """
            MERGE (e:Entity {external_id: $ext_id, type: $obj_type})
            MERGE (s:Snapshot {snap_id: $snap_id})
            CREATE (e)-[:HAS_VERSION {hash: $hash, at: datetime()}]->(s)
            """
            try:
                session.run(
                    query,
                    ext_id=external_id,
                    obj_type=object_type,
                    snap_id=snapshot_id,
                    hash=item_hash,
                )
            except Exception as e:
                logger.error(f"❌ Erreur Neo4j (update_relation) : {e}")

    # ================== LIENS GÉNÉRIQUES ==================

    def link_entities(self, from_id: str, from_type: str, to_id: str, to_type: str, relation_name: str):
        """
        Relie deux entités de n'importe quel type avec n'importe quelle étiquette.
        Ex: link_entities("123", "deals", "456", "companies", "ASSOCIATED_WITH")
        """
        with self.driver.session() as session:
            query = f"""
            MERGE (a:Entity {{external_id: $a_id, type: $a_type}})
            MERGE (b:Entity {{external_id: $b_id, type: $b_type}})
            MERGE (a)-[:{relation_name.upper()}]->(b)
            """
            try:
                session.run(
                    query,
                    a_id=from_id,
                    a_type=from_type,
                    b_id=to_id,
                    b_type=to_type,
                )
            except Exception as e:
                logger.error(f"❌ Erreur Neo4j (link_entities) : {e}")

    # ================== ORPHANS & RELATIONS ==================

    def get_entity_relations(self, object_type: str, external_id: str, snapshot_id: int) -> Dict[str, List[str]]:
        """
        Récupère toutes les relations sortantes d'une entité (hors HAS_VERSION).
        Retourne { entity_type: [external_id1, external_id2, ...] }.
        """
        with self.driver.session() as session:
            query = """
            MATCH (e:Entity {external_id: $ext_id, type: $obj_type})-[r]->(related:Entity)
            WHERE NOT type(r) = 'HAS_VERSION'
            RETURN type(r) as rel_type, related.type as entity_type, related.external_id as entity_id
            """
            try:
                result = session.run(query, ext_id=external_id, obj_type=object_type)
                relations: Dict[str, List[str]] = {}
                for record in result:
                    e_type = record["entity_type"]
                    e_id = record["entity_id"]
                    relations.setdefault(e_type, []).append(e_id)
                return relations
            except Exception as e:
                logger.error(f"❌ Erreur Neo4j (get_entity_relations) : {e}")
                return {}

    def check_orphans(
        self,
        object_type: str,
        external_id: str,
        snapshot_id: int,
        current_crm_ids: Set[str],
    ) -> Dict[str, List[str]]:
        """
        Vérifie quelles entités liées n'existent plus dans le CRM actuel.
        Retourne par ex : {"missing_companies": ["123", "456"]}.
        """
        historical = self.get_entity_relations(object_type, external_id, snapshot_id)
        orphans: Dict[str, List[str]] = {}
        for rel_type, ids in historical.items():
            missing = [oid for oid in ids if oid not in current_crm_ids]
            if missing:
                orphans[f"missing_{rel_type}"] = missing
        return orphans

    # ================== ANALYSE & VISU ==================

    def get_impact_analysis(self, object_type: str, external_id: str, snapshot_id: int) -> Dict:
        """Analyse la complexité relationnelle d'une entité pour un snapshot donné."""
        relations = self.get_entity_relations(object_type, external_id, snapshot_id)
        relation_count = sum(len(ids) for ids in relations.values())

        complexity = "low"
        if relation_count > 10:
            complexity = "high"
        elif relation_count > 3:
            complexity = "medium"

        return {
            "entity": {"type": object_type, "id": external_id},
            "historical_relations": relations,
            "relation_count": relation_count,
            "complexity": complexity,
        }

    def visualize_entity_graph(self, object_type: str, external_id: str, snapshot_id: int) -> str:
        """Représentation ASCII simple du graphe de l'entité."""
        relations = self.get_entity_relations(object_type, external_id, snapshot_id)
        if not relations:
            return f"Aucune relation trouvée pour {object_type} #{external_id} dans le Snap #{snapshot_id}"

        lines = [f"┌─ {object_type.upper()} #{external_id}"]
        for rel_type, ids in relations.items():
            lines.append(f"├─ {rel_type.upper()} ({len(ids)})")
            for i, eid in enumerate(ids):
                connector = "└─" if i == len(ids) - 1 else "├─"
                lines.append(f"│  {connector} #{eid}")
        return "\n".join(lines)
