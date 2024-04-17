# require CREATE INDEX batch_for_events FOR (e:Event) ON (e.batch)
# CREATE INDEX batch_for_kits FOR (e:Kit) ON (e.kitId)

# option
# CALL apoc.periodic.iterate(
# "MATCH (n:BatchInstance) RETURN n",
# "UNWIND n.kits AS id_val MATCH (k:Kit) WHERE id_val = k.kitId MERGE (n)-[:CORR]->(k)",
# {batchSize:100, parallel:false}))
# takes a lot of time

from neo4j import GraphDatabase


class BatchInstanceRelationshipCreator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

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
            # Connect BatchInstance to Resource based on resource sysId
            session.write_transaction(self._connect_batch_instance_to_resource)
            print("Query to connect BatchInstance to Resource executed.")
            # Connect BatchInstance to Event based on batch_number
            session.write_transaction(self._connect_batch_instance_to_event)
            print("Query to connect BatchInstance to Event executed.")
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
    def _connect_batch_instance_to_resource(tx):
        tx.run("""
                CALL apoc.periodic.iterate(
                "MATCH (n:BatchInstance) RETURN n",
                "UNWIND n.resource_sys_id AS id_val MATCH (e:Resource) WHERE id_val = e.sysId MERGE (e)-[:CORR]->(n)",
                {batchSize:100})
            """)

    @staticmethod
    def _connect_batch_instance_to_event(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (n:BatchInstance) RETURN n",
            "UNWIND n.batch_number AS id_val MATCH (e:Event) WHERE id_val = e.batch MERGE (e)-[:CORR]->(n)",
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
            MATCH (n:BatchInstance)  
            MATCH (n)<-[:CORR]-(u:Resource)  
            WITH u, n AS nodes ORDER BY n.earliest_timestamp, ID(n)  
            WITH u, collect(nodes) AS batch_node_list  
            UNWIND range(0, size(batch_node_list)-2) AS i  
            WITH u, batch_node_list[i] AS b1, batch_node_list[i+1] AS b2  
            MERGE (b1)-[df:DF_BATCH_RESOURCE]->(b2)  
            """)

def main():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "12345678"
    creator = BatchInstanceRelationshipCreator(uri, user, password)

    try:
        creator.create_indexes()
        creator.create_relationships()
    finally:
        creator.close()


if __name__ == "__main__":
    main()
