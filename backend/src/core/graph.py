from src.utils.db import neo4j_driver
from loguru import logger
from typing import Dict, List, Set

class GraphManager:
    def __init__(self):
        self.driver = neo4j_driver

    def update_relation(self, snapshot_id: int, object_type: str, external_id: str, item_hash: str):
        with self.driver.session() as session:
            query = """
            MERGE (e:Entity {external_id: $ext_id, type: $obj_type})
            MERGE (s:Snapshot {snap_id: $snap_id})
            CREATE (e)-[:HAS_VERSION {hash: $hash, at: datetime()}]->(s)
            """
            try:
                session.run(query, 
                    ext_id=external_id, 
                    obj_type=object_type, 
                    snap_id=snapshot_id, 
                    hash=item_hash
                )
            except Exception as e:
                logger.error(f"❌ Erreur Neo4j : {e}")

    def create_belongs_to(self, contact_ext_id: str, company_ext_id: str):
        with self.driver.session() as session:
            query = """
            MERGE (c:Entity {external_id: $c_id, type: 'contacts'})
            MERGE (co:Entity {external_id: $co_id, type: 'companies'})
            MERGE (c)-[:WORKS_AT]->(co)
            """
            session.run(query, c_id=contact_ext_id, co_id=company_ext_id)            

    def create_deal_relations(self, deal_id: str, company_id: str = None, contact_id: str = None):
        with self.driver.session() as session:
            if company_id:
                query_co = """
                MATCH (d:Entity {external_id: $d_id, type: 'deals'})
                MATCH (co:Entity {external_id: $co_id, type: 'companies'})
                MERGE (d)-[:ASSOCIATED_WITH]->(co)
                """
                session.run(query_co, d_id=deal_id, co_id=company_id)

            if contact_id:
                query_c = """
                MATCH (d:Entity {external_id: $d_id, type: 'deals'})
                MATCH (c:Entity {external_id: $c_id, type: 'contacts'})
                MERGE (d)-[:INVOLVES]->(c)
                """
                session.run(query_c, d_id=deal_id, c_id=contact_id)    

    def create_ticket_relations(self, ticket_id: str, contact_id: str = None, company_id: str = None):
        with self.driver.session() as session:
            if contact_id:
                query_c = """
                MATCH (t:Entity {external_id: $t_id, type: 'tickets'})
                MATCH (c:Entity {external_id: $c_id, type: 'contacts'})
                MERGE (t)-[:REPORTED_BY]->(c)
                """
                session.run(query_c, t_id=ticket_id, c_id=contact_id)

            if company_id:
                query_co = """
                MATCH (t:Entity {external_id: $t_id, type: 'tickets'})
                MATCH (co:Entity {external_id: $co_id, type: 'companies'})
                MERGE (t)-[:CONCERNS]->(co)
                """
                session.run(query_co, t_id=ticket_id, co_id=company_id)

    # ========================================
    # NOUVELLES FONCTIONS : ANALYSE D'IMPACT
    # ========================================

    def get_entity_relations(self, object_type: str, external_id: str, snapshot_id: int) -> Dict[str, List[str]]:
        """
        Récupère toutes les relations d'une entité dans un snapshot donné.
        
        Returns:
            {
                "companies": ["123", "456"],
                "contacts": ["789"],
                "deals": ["101"]
            }
        """
        with self.driver.session() as session:
            query = """
            MATCH (e:Entity {external_id: $ext_id, type: $obj_type})-[r]->(related:Entity)
            WHERE EXISTS {
                MATCH (e)-[:HAS_VERSION]->(s:Snapshot {snap_id: $snap_id})
            }
            RETURN type(r) as rel_type, related.type as entity_type, related.external_id as entity_id
            """
            
            result = session.run(query, ext_id=external_id, obj_type=object_type, snap_id=snapshot_id)
            
            relations = {}
            for record in result:
                entity_type = record["entity_type"]
                entity_id = record["entity_id"]
                
                if entity_type not in relations:
                    relations[entity_type] = []
                relations[entity_type].append(entity_id)
            
            return relations

    def check_orphans(self, object_type: str, external_id: str, snapshot_id: int, current_entities: Set[str]) -> Dict[str, List[str]]:
        """
        Détecte les orphelins : relations du snapshot qui n'existent plus dans le CRM actuel.
        
        Args:
            object_type: Type de l'objet à restaurer
            external_id: ID de l'objet à restaurer
            snapshot_id: Snapshot source
            current_entities: Set des IDs actuellement présents dans le CRM
        
        Returns:
            {
                "missing_companies": ["123"],
                "missing_contacts": ["789"],
                "missing_deals": []
            }
        """
        # Récupérer les relations historiques
        historical_relations = self.get_entity_relations(object_type, external_id, snapshot_id)
        
        orphans = {}
        
        for entity_type, entity_ids in historical_relations.items():
            missing = [eid for eid in entity_ids if eid not in current_entities]
            if missing:
                orphans[f"missing_{entity_type}"] = missing
        
        return orphans

    def get_impact_analysis(self, object_type: str, external_id: str, snapshot_id: int) -> Dict:
        """
        Analyse complète de l'impact d'une restauration.
        
        Returns:
            {
                "entity": {"type": "contacts", "id": "123"},
                "historical_relations": {
                    "companies": ["456"],
                    "deals": ["789"]
                },
                "relation_count": 2,
                "complexity": "medium"  # low/medium/high
            }
        """
        relations = self.get_entity_relations(object_type, external_id, snapshot_id)
        relation_count = sum(len(ids) for ids in relations.values())
        
        # Déterminer la complexité
        if relation_count == 0:
            complexity = "low"
        elif relation_count <= 5:
            complexity = "medium"
        else:
            complexity = "high"
        
        return {
            "entity": {"type": object_type, "id": external_id},
            "historical_relations": relations,
            "relation_count": relation_count,
            "complexity": complexity
        }

    def visualize_entity_graph(self, object_type: str, external_id: str, snapshot_id: int) -> str:
        """
        Génère une représentation ASCII de l'entité et ses relations.
        
        Returns:
            String ASCII art du graphe
        """
        relations = self.get_entity_relations(object_type, external_id, snapshot_id)
        
        graph = []
        graph.append(f"\n┌─ {object_type.upper()} #{external_id}")
        
        for rel_type, entity_ids in relations.items():
            graph.append(f"│")
            graph.append(f"├─ {rel_type.upper()} ({len(entity_ids)})")
            for eid in entity_ids:
                graph.append(f"│  └─ #{eid}")
        
        graph.append("└─")
        
        return "\n".join(graph)