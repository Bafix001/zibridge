from typing import Dict, List, Set, Optional
from loguru import logger
from src.utils.db import neo4j_driver

class GraphManager:
    def __init__(self):
        self.driver = neo4j_driver
        self._setup_indices()

    def _setup_indices(self):
        """Contraintes pour garantir l'intégrité et la vitesse des recherches."""
        if not self.driver: return
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE (e.external_id, e.type, e.project_id) IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.type)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Snapshot) ON (s.snap_id)"
        ]
        with self.driver.session() as session:
            for q in queries:
                try: 
                    session.run(q)
                except Exception as e:
                    logger.debug(f"Index/Contrainte : {e}")

    # ================== 🧹 CLEANUP (LA MÉTHODE MANQUANTE) ==================

    def clear_project_graph(self, project_id: int):
        """
        Supprime les nœuds et relations liés à ce projet pour repartir à neuf.
        Utilisé au début de chaque synchronisation.
        """
        if not self.driver: return
        query = "MATCH (n:Entity {project_id: $project_id}) DETACH DELETE n"
        with self.driver.session() as session:
            session.run(query, project_id=project_id)
            logger.info(f"🧹 Graphe Neo4j nettoyé pour le projet {project_id}")

    # ================== 🚀 BATCH PROCESSING (TURBO) ==================

    def link_entities_batch(self, project_id: int, from_type: str, links: List[dict]):
        """
        🔥 Suture Turbo : Crée des milliers de liens en une seule transaction.
        'links' attendu: [{'from_id': 'A', 'to_id': 'B', 'to_type': 'Company', 'role': 'PRIMARY'}]
        """
        if not links or not self.driver: return

        query = """
        UNWIND $batch as row
        MERGE (a:Entity {external_id: row.from_id, type: $from_type, project_id: $project_id})
        MERGE (b:Entity {external_id: row.to_id, type: row.to_type, project_id: $project_id})
        MERGE (a)-[r:LINKED_TO]->(b)
        SET r.role = row.role, r.updated_at = datetime()
        """
        with self.driver.session() as session:
            try:
                session.run(query, batch=links, from_type=from_type, project_id=project_id)
            except Exception as e:
                logger.error(f"❌ Neo4j Batch Error: {e}")

    # ================== 🧬 LOGIQUE DE SUTURE & RESTAURATION ==================

    def get_restoration_order(self, project_id: int) -> List[str]:
        """
        📐 TRI TOPOLOGIQUE (Elon Mode) :
        Analyse les dépendances pour dire au moteur dans quel ordre créer les objets.
        """
        if not self.driver: return []
        
        query = """
        MATCH (a:Entity {project_id: $project_id})-[r:LINKED_TO]->(b:Entity)
        WHERE a.type <> b.type
        RETURN DISTINCT a.type as source, b.type as target
        """
        with self.driver.session() as session:
            result = session.run(query, project_id=project_id)
            deps = {}
            all_types = set()
            for record in result:
                src, tgt = record["source"], record["target"]
                deps.setdefault(src, set()).add(tgt)
                all_types.update([src, tgt])
            
            ordered = []
            visited = set()

            def visit(t):
                if t not in visited:
                    visited.add(t)
                    for child in deps.get(t, []):
                        visit(child)
                    ordered.insert(0, t)

            for t in all_types:
                visit(t)
            
            return ordered[::-1]

    def get_orphan_entities(self, project_id: int, source_type: str, target_type: str) -> List[str]:
        """Trouve les entités (ex: Deals) sans parents (ex: Company)."""
        query = """
        MATCH (s:Entity {type: $source_type, project_id: $project_id})
        WHERE NOT (s)-[:LINKED_TO]->(:Entity {type: $target_type, project_id: $project_id})
        RETURN s.external_id as id
        """
        with self.driver.session() as session:
            result = session.run(query, source_type=source_type, target_type=target_type, project_id=project_id)
            return [record["id"] for record in result]
        
    
    def create_relation(self, *args, **kwargs):
        """
        🔗 Suture Ultra-Flexible (Elon Mode).
        Gère les appels positionnels et par mots-clés.
        """
        if not self.driver: return

        # Extraction intelligente des arguments
        # On s'attend à : from_id, from_type, to_id, to_type, rel_type...
        from_id = args[0] if len(args) > 0 else kwargs.get('from_id')
        from_type = args[1] if len(args) > 1 else kwargs.get('from_type')
        to_id = args[2] if len(args) > 2 else kwargs.get('to_id')
        to_type = args[3] if len(args) > 3 else kwargs.get('to_type')
        
        # Le project_id peut être passé en position 5 ou via kwargs
        p_id = kwargs.get('project_id') or (args[5] if len(args) > 5 else None)
        rel_type = kwargs.get('rel_type') or (args[4] if len(args) > 4 else "ASSOCIATED")

        if not p_id:
            # Si on ne l'a toujours pas, on essaie de le trouver dans le contexte global ou on utilise un fallback
            return

        query = """
        MERGE (a:Entity {external_id: $from_id, type: $from_type, project_id: $p_id})
        MERGE (b:Entity {external_id: $to_id, type: $to_type, project_id: $p_id})
        MERGE (a)-[r:LINKED_TO]->(b)
        SET r.role = $rel_type, r.updated_at = datetime()
        """
        with self.driver.session() as session:
            session.run(query, p_id=p_id, from_id=str(from_id), from_type=from_type, 
                        to_id=str(to_id), to_type=to_type, rel_type=rel_type)    