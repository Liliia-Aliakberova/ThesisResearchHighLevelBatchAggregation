"""
The code is dedicated for defining the logic of batch over resource assigning batch values to Event nodes
"""

from datetime import datetime, timedelta
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

class EventBatchAssigner:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def fetch_events(self):
        with self.driver.session() as session:
            result = session.run("""
            MATCH (u:Resource)<-[:CORR]-(e:Event)-[:CORR]->(k:Kit)
            RETURN id(e) AS id, e.activity AS activity, datetime(e.timestamp).epochMillis AS timestamp, u.sysId AS resourceSysId
            ORDER BY  e.timestamp, resourceSysId ASC
            """)
            return [{**record} for record in result]

    def process_and_update_events(self, events):
        if not events:
            return

        current_batch = 1
        # Mark the first event with the initial batch number
        events[0]['batch'] = current_batch
        prev_event = events[0]

        # Start from the second event
        for event in events[1:]:
            event_time = datetime.fromtimestamp(event['timestamp'] / 1000.0)
            prev_event_time = datetime.fromtimestamp(prev_event['timestamp'] / 1000.0)

            if (event['resourceSysId'] == prev_event['resourceSysId'] and
                event['activity'] == prev_event['activity'] and
                (event_time - prev_event_time) < timedelta(minutes=5)):
                event['batch'] = current_batch
            else:
                current_batch += 1
                event['batch'] = current_batch
            
            prev_event = event

        # Update all events in the database after batch assignment
        for event in events:
            self.update_event_batch(event['id'], event['batch'])
            print(f"Updated event {event['id']} with batch {event['batch']}")

    def update_event_batch(self, event_id, batch_number):
        with self.driver.session() as session:
            session.write_transaction(lambda tx: tx.run("""
            MATCH (e:Event) WHERE id(e) = $event_id
            SET e.batch = $batch_number
            """, event_id=event_id, batch_number=batch_number))

def main():
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    assigner = EventBatchAssigner(uri, username, password)

    try:
        print("Fetching events...")
        events = assigner.fetch_events()
        print(f"Fetched {len(events)} events. Processing and updating...")
        assigner.process_and_update_events(events)
    finally:
        assigner.close()

if __name__ == "__main__":
    main()
