"""
The code is dedicated for Batch nodes creation, named BatchInstances
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

class BatchInstanceCreator:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def create_batch_instances(self):
        with self.driver.session() as session:
            result = session.run("""
            MATCH (u:Resource)<-[:CORR]-(e:Event)-[:CORR]->(k:Kit)
            WITH e.batch AS batch_number, e.activity AS activity, u.sysId AS resource_sys_id,
                 COLLECT(DISTINCT k.kitId) AS kits, COLLECT(DISTINCT k.runId) AS runs,
                 MIN(e.timestamp) AS earliest_timestamp, MAX(e.timestamp) AS latest_timestamp,
                 COUNT(DISTINCT k.kitId) AS kit_count, COUNT(e) AS event_count
            WHERE batch_number IS NOT NULL 
            CREATE (batchInstance:BatchInstance {
                batch_number: batch_number,
                activity: activity,
                kits: kits,
                kits_number: kit_count,
                event_number: event_count,
                resource_sys_id: resource_sys_id,
                runs: CASE WHEN size(runs) > 0 THEN runs ELSE [] END,
                earliest_timestamp: earliest_timestamp,
                latest_timestamp: latest_timestamp
            })
            RETURN COUNT(batchInstance) AS created_instances
            """)

            created_instances = result.single()
            created_instances_count = created_instances["created_instances"] if created_instances else 0
            print(f"{created_instances_count} BatchInstance nodes created.")

def main():
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    creator = BatchInstanceCreator(uri, username, password)

    try:
        creator.create_batch_instances()
    finally:
        creator.close()

if __name__ == "__main__":
    main()
