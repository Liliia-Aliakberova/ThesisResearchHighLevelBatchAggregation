"""
The code is dedicated for constracting directly-follow and correlated relations 
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

class BatchInstanceRelationshipCreator:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def create_indexes(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_event_index)
            session.write_transaction(self._create_kit_index)
            print("Indexes creation queries executed.")

    @staticmethod
    def _create_event_index(tx):
        tx.run("CREATE INDEX batch_for_events IF NOT EXISTS FOR (e:Event) ON (e.batch)")

    @staticmethod
    def _create_kit_index(tx):
        tx.run("CREATE INDEX batch_for_kits IF NOT EXISTS FOR (e:Kit) ON (e.kitId)")

    def create_relationships(self):
        with self.driver.session() as session:
            # Connect BatchInstance to Event based on batch_number
            session.write_transaction(self._connect_batch_instance_to_event)
            print("Query to connect BatchInstance to Event executed.")
            # Connect BatchInstance to Resource based on resource sysId
            session.write_transaction(self._connect_batch_instance_to_resource)
            print("Query to connect BatchInstance to Resource executed.")
            # Connect BatchInstance to Kit based on kits
            session.write_transaction(self._connect_batch_instance_to_kit)
            print("Query to connect BatchInstance to Kit executed.")
            # Create DF edges between BatchInstances related to Kits
            session.write_transaction(self._connect_batch_instances_kit)
            print("Query to connect BatchInstance executed.")
            # Create DF edges between BatchInstances related to Resource
            session.write_transaction(self._connect_batch_instances_resource)
            print("Query to connect BatchInstance executed.")


    @staticmethod
    def _connect_batch_instance_to_event(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (n:BatchInstance) RETURN n",
            "UNWIND n.batch_number AS id_val MATCH (e:Event) WHERE id_val = e.batch MERGE (e)-[:CORR]->(n)",
            {batchSize:100})

        """)

    @staticmethod
    def _connect_batch_instance_to_resource(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (e:Event)-[:CORR]->(n:BatchInstance) RETURN e, n",
            "MATCH (e)-[:CORR]->(u:Resource) MERGE (u)-[:CORR]->(n)",
            {batchSize:100})
               """)

    @staticmethod
    def _connect_batch_instance_to_kit(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (e:Event)-[:CORR]->(n:BatchInstance) RETURN e, n",
            "MATCH (e)-[:CORR]->(k:Kit) MERGE (k)-[:CORR]->(n)",
            {batchSize:100})

        """)

    @staticmethod
    def _connect_batch_instances_kit(tx):
        tx.run("""
        MATCH (k:Kit)<-[:CORR]-(e:Event)-[:DF_KIT]->(e1:Event)-[:CORR]->(k)
        MATCH (e)-[:CORR]->(n:BatchInstance), (e1)-[:CORR]->(n1:BatchInstance)
        WHERE n.batch_number = e.batch AND n1.batch_number = e1.batch AND e.batch <> e1.batch
        WITH n, n1, k.kitId AS kitId, k.runId AS runId
        CALL apoc.do.when(
            runId IS NOT NULL,
            "MERGE (n)-[r:DF_BATCH_KIT {kitId: $kitId, runId: $runId}]->(n1) RETURN r",
            "MERGE (n)-[r:DF_BATCH_KIT {kitId: $kitId}]->(n1) RETURN r",
            {n:n, n1:n1, kitId:kitId, runId:runId}
        )YIELD value
        RETURN count(value) AS newRelationshipsCreated
        """)

    @staticmethod
    def _connect_batch_instances_resource(tx):
        tx.run("""            
                MATCH (u:Resource)<-[:CORR]-(e:Event)-[:DF_RESOURCE]->(e1:Event)-[:CORR]->(u)
                MATCH (e)-[:CORR]->(n:BatchInstance), (e1)-[:CORR]->(n1:BatchInstance)
                WHERE e.batch <> e1.batch AND date(n.earliest_timestamp) = date(n1.earliest_timestamp)
                AND u.sysId IN n.users AND u.sysId IN n1.users
                WITH n, n1, u.sysId AS sysId, COUNT(*) AS transitions, date(n.earliest_timestamp) AS event_date, e.timestamp AS e_timestamp, ID(e) as event_id
                ORDER BY sysId,event_date, e_timestamp, event_id 

                WITH COLLECT({n: n, n1: n1, sysId: sysId, transitions: transitions, event_date: event_date}) AS edges
                UNWIND edges AS edge
                WITH edge.n AS n, edge.n1 AS n1, edge.sysId AS sysId, edge.transitions AS transitions, edge.event_date AS event_date

                CALL {
                    WITH n, n1, sysId, transitions, event_date
                    //Check for existing edges with the same sysId and event_date to calculate order
                    OPTIONAL MATCH (:BatchInstance)-[existing:DF_BATCH_RESOURCE {sysId: sysId}]->(:BatchInstance)
                    WHERE date(existing.created_at) = event_date
                    WITH n, n1, sysId, transitions, event_date, 
                        COUNT(existing) AS existing_count,
                        CASE WHEN max(existing.order) IS NULL THEN 0 ELSE max(existing.order) END AS max_order
                    //Check for existing edges from the same source node to calculate outgoing_order
                    OPTIONAL MATCH (n)-[existing_out:DF_BATCH_RESOURCE {sysId: sysId}]->()
                    WHERE date(existing_out.created_at) = event_date
                    WITH n, n1, sysId, transitions, event_date, existing_count, max_order,
                        CASE WHEN max(existing_out.outgoing_order) IS NULL THEN 0 ELSE max(existing_out.outgoing_order) END AS max_outgoing_order
                    //Create or update the edge with the calculated order and outgoing_order
                    MERGE (n)-[r:DF_BATCH_RESOURCE {sysId: sysId, created_at: event_date}]->(n1)
                    ON CREATE SET r.count = transitions, r.order = existing_count + 1, r.outgoing_order = max_outgoing_order + 1
                    ON MATCH SET r.count = r.count + transitions
                    RETURN r
                            }
                    RETURN r, r.count AS newRelationshipsCreated, r.order AS newOrder, r.outgoing_order AS newOutgoingOrder
               """)

def main():
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    creator = BatchInstanceRelationshipCreator(uri, username, password)

    try:
        creator.create_indexes()
        creator.create_relationships()
    finally:
        creator.close()


if __name__ == "__main__":
    main()

