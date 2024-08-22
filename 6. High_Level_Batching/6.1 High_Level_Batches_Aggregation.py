"""The code dedicated to the high-level aggregation. 
It consists of high-level batch creation and construction 
of directly-follow and correlated relations
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Connect to the Neo4j database
load_dotenv()
uri = os.getenv('NEO4J_URI')
username = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASSWORD')
driver = GraphDatabase.driver(uri, auth=(username, password))

def delete_high_level_batches(tx):
    # Delete existing HighLevelBatch nodes and their edges
    query = """
    MATCH (hlb:HighLevelBatch)
    DETACH DELETE hlb
    """
    tx.run(query)
    print("Deleted existing HighLevelBatch nodes and edges.")

def create_high_level_batches_v2(tx):
    # Step 1: Fetch user paths for each day
    user_paths_query = """
    MATCH (u:Resource)-[:CORR]->(batch:BatchInstance)-[r1:DF_BATCH_RESOURCE]->(nextBatch:BatchInstance)<-[:CORR]-(u)
    WHERE batch <> nextBatch AND u.sysId = r1.sysId
    RETURN u, batch, nextBatch, date(batch.earliest_timestamp) AS eventDate, r1.sysId AS sysId, r1.count AS count1, r1.order
    ORDER BY r1.order

    """

    try:
        user_paths_result = tx.run(user_paths_query)
    except Exception as e:
        print(f"Error running user paths query: {e}")
        return
    
    hlb_count = 0
    failed_count = 0
    created_nodes = set()

    try:
        for record in user_paths_result:
            sysId = record["sysId"]
            eventDate = record["eventDate"]
            batch = record["batch"]
            next_batch = record["nextBatch"]

            if batch and next_batch:
                batch_number = batch["batch_number"]
                next_batch_number = next_batch["batch_number"]

                # Sort batch numbers in ascending order
                sorted_batch_numbers = tuple(sorted([batch_number, next_batch_number]))

                # Check if a node with the same sorted corr_batch_numbers already exists for the user
                existing_node_query = """
                MATCH (hlb:HighLevelBatch {sysId: $sysId})
                WHERE apoc.coll.sort(hlb.corr_batch_numbers) = $sorted_batch_numbers
                RETURN hlb
                """

                try:
                    existing_node_result = tx.run(existing_node_query, sysId=sysId, sorted_batch_numbers=sorted_batch_numbers)
                    existing_node_record = existing_node_result.single()
                except Exception as e:
                    print(f"Error running existing node query: {e}")
                    continue

                if existing_node_record:
                    # Node exists, skip processing
                    created_nodes.add(sorted_batch_numbers)
                    continue
                else:
                    # Check for a loop: batch -> nextBatch and nextBatch -> batch
                    reverse_loop_query = """
                    MATCH (u:Resource)-[:CORR]->(nextBatch:BatchInstance)-[r1:DF_BATCH_RESOURCE {sysId: $sysId}]->(batch:BatchInstance)<-[:CORR]-(u)
                    MATCH (u)-[:CORR]->(batch)-[r2:DF_BATCH_RESOURCE {sysId: $sysId}]->(nextBatch)<-[:CORR]-(u)
                    WHERE batch.batch_number = $batch_number
                      AND nextBatch.batch_number = $next_batch_number
                      AND r1.sysId = r2.sysId
                    RETURN u, batch, nextBatch
                    """

                    try:
                        reverse_loop_result = tx.run(reverse_loop_query, batch_number=batch_number, next_batch_number=next_batch_number, sysId=sysId)
                        reverse_loop_exists = reverse_loop_result.single() is not None
                    except Exception as e:
                        print(f"Error running reverse loop query: {e}")
                        continue

                    if reverse_loop_exists:
                        # There is a loop, check if individual nodes exist and merge into a joint node
                        joint_activity_names = {batch["activity"], next_batch["activity"]}
                        all_batches = set(sorted_batch_numbers)

                        # Check if either batch is already part of a joint node or exists as an individual node
                        existing_joint_or_individual_query = """
                        MATCH (hlb:HighLevelBatch {sysId: $sysId})
                        WHERE ANY(batchNum IN hlb.corr_batch_numbers WHERE batchNum IN $sorted_batch_numbers)
                        RETURN hlb
                        """

                        try:
                            existing_joint_or_individual_result = tx.run(existing_joint_or_individual_query, sysId=sysId, sorted_batch_numbers=sorted_batch_numbers)
                            existing_joint_or_individual_records = existing_joint_or_individual_result.data()
                        except Exception as e:
                            print(f"Error running existing joint or individual query: {e}")
                            continue

                        if existing_joint_or_individual_records:
                            # Before merging, ensure no existing node has all these batch numbers
                            fully_overlapping_query = """
                            MATCH (hlb:HighLevelBatch {sysId: $sysId})
                            WHERE all(batchNum IN $sorted_batch_numbers WHERE batchNum IN hlb.corr_batch_numbers)
                            RETURN hlb
                            """
                            try:
                                fully_overlapping_result = tx.run(fully_overlapping_query, sysId=sysId, sorted_batch_numbers=sorted_batch_numbers)
                                fully_overlapping_record = fully_overlapping_result.single()
                            except Exception as e:
                                print(f"Error checking fully overlapping nodes: {e}")
                                continue

                            if fully_overlapping_record:
                                # Fully overlapping node exists, skip creation
                                continue

                            merged_batches = set()
                            merged_activities = set()
                            for rec in existing_joint_or_individual_records:
                                existing_node = rec['hlb']
                                merged_batches.update(existing_node['corr_batch_numbers'])
                                merged_activities.update(existing_node['activity_name'])

                            # Update with new batches and activities
                            merged_batches.update(all_batches)
                            merged_activities.update(joint_activity_names)

                            # Update the merged node
                            update_existing_query = """
                            MATCH (hlb:HighLevelBatch {sysId: $sysId})
                            WHERE ANY(batchNum IN hlb.corr_batch_numbers WHERE batchNum IN $sorted_batch_numbers)
                            SET hlb.corr_batch_numbers = $merged_batches,
                                hlb.activity_name = $merged_activities
                            RETURN hlb
                            """
                            try:
                                hlb_result = tx.run(update_existing_query, sysId=sysId, sorted_batch_numbers=sorted_batch_numbers, merged_batches=list(merged_batches), merged_activities=list(merged_activities))
                                hlb_record = hlb_result.single()
                            except Exception as e:
                                print(f"Error updating existing node: {e}")
                                failed_count += 1
                                continue
                        else:
                            # Create a new joint node
                            joint_node_query = """
                            MERGE (hlb:HighLevelBatch {sysId: $sysId, corr_batch_numbers: $sorted_batch_numbers})
                            ON CREATE SET hlb.activity_name = $joint_activity_names,
                                          hlb.date = $eventDate
                            RETURN hlb
                            """
                            try:
                                hlb_result = tx.run(joint_node_query, sysId=sysId, sorted_batch_numbers=sorted_batch_numbers, joint_activity_names=list(joint_activity_names), eventDate=eventDate)
                                hlb_record = hlb_result.single()
                            except Exception as e:
                                print(f"Error creating or merging joint node: {e}")
                                failed_count += 1
                                continue

                            if hlb_record:
                                created_nodes.add(sorted_batch_numbers)
                                hlb_count += 1
                                print(f"Created or merged node: {hlb_record['hlb']}")
                            else:
                                failed_count += 1
                    else:
                        # Create individual nodes for non-loops
                        for b in [batch, next_batch]:
                            batch_number = b["batch_number"]
                            activity_name = b["activity"]

                            # Check if this new node participates in any joint node
                            existing_joint_query = """
                            MATCH (hlb:HighLevelBatch {sysId: $sysId})
                            WHERE $batch_number IN hlb.corr_batch_numbers
                            RETURN hlb
                            """
                            try:
                                existing_joint_result = tx.run(existing_joint_query, sysId=sysId, batch_number=batch_number)
                                existing_joint_record = existing_joint_result.single()
                            except Exception as e:
                                print(f"Error running existing joint query: {e}")
                                continue
                            
                            if existing_joint_record:
                                # Append this node to the existing joint node
                                update_joint_query = """
                                MATCH (hlb:HighLevelBatch {sysId: $sysId})
                                WHERE $batch_number IN hlb.corr_batch_numbers
                                SET hlb.corr_batch_numbers = apoc.coll.union(hlb.corr_batch_numbers, [$batch_number]),
                                    hlb.activity_name = apoc.coll.union(hlb.activity_name, [$activity_name])
                                RETURN hlb
                                """
                                try:
                                    update_joint_result = tx.run(update_joint_query, sysId=sysId, batch_number=batch_number, activity_name=activity_name)
                                    hlb_record = update_joint_result.single()
                                except Exception as e:
                                    print(f"Error updating joint node: {e}")
                                    failed_count += 1
                                    continue
                            else:
                                # Check if there is another individual node with the same batch id
                                existing_individual_query = """
                                MATCH (hlb:HighLevelBatch {sysId: $sysId, corr_batch_numbers: [$batch_number]})
                                RETURN hlb
                                """
                                try:
                                    existing_individual_result = tx.run(existing_individual_query, sysId=sysId, batch_number=batch_number)
                                    existing_individual_record = existing_individual_result.single()
                                except Exception as e:
                                    print(f"Error running existing individual query: {e}")
                                    continue
                                
                                if existing_individual_record:
                                    continue  # Skip creation if an individual node already exists
                                
                                # Create a new individual HighLevelBatch node
                                individual_node_query = """
                                MERGE (hlb:HighLevelBatch {
                                    sysId: $sysId,
                                    activity_name: [$activity_name],
                                    corr_batch_numbers: [$batch_number]
                                })
                                ON CREATE SET hlb.date = $eventDate
                                RETURN hlb
                                """
                                try:
                                    individual_node_result = tx.run(individual_node_query, sysId=sysId, activity_name=activity_name, batch_number=batch_number, eventDate=eventDate)
                                    hlb_record = individual_node_result.single()
                                except Exception as e:
                                    print(f"Error creating individual node: {e}")
                                    failed_count += 1
                                    continue
                            
                            if hlb_record:
                                hlb_count += 1
                                created_nodes.add(tuple(sorted([batch_number])))
                                print(f"Created node: {hlb_record['hlb']}")
                            else:
                                failed_count += 1
    except Exception as e:
        print(f"Error processing user paths result: {e}")
    
    # Handle resources with only one batch instance in a day
    single_batch_query = """
    MATCH (u:Resource)-[:CORR]->(batch:BatchInstance)
    WHERE u.sysId IN batch.users
    WITH u, batch, date(batch.earliest_timestamp) AS eventDate
    WITH u, eventDate, COUNT(batch) AS batchCount, COLLECT(batch) AS batches
    WHERE batchCount = 1
    UNWIND batches AS batch
    RETURN u, batch, eventDate, u.sysId AS sysId
    """

    try:
        single_batch_result = tx.run(single_batch_query)
    except Exception as e:
        print(f"Error running single batch query: {e}")
        return

    try:
        for record in single_batch_result:
            sysId = record["sysId"]
            eventDate = record["eventDate"]
            batch = record["batch"]

            if (sysId, eventDate) not in created_nodes:
                batch_number = batch["batch_number"]
                activity_name = batch["activity"]

                # Check if an individual node already exists
                existing_individual_query = """
                MATCH (hlb:HighLevelBatch {sysId: $sysId, corr_batch_numbers: [$batch_number]})
                RETURN hlb
                """
                try:
                    existing_individual_result = tx.run(existing_individual_query, sysId=sysId, batch_number=batch_number)
                    existing_individual_record = existing_individual_result.single()
                except Exception as e:
                    print(f"Error running existing individual query: {e}")
                    continue
                
                if existing_individual_record:
                    continue  # Skip creation if an individual node already exists

                # Create a new individual HighLevelBatch node
                individual_node_query = """
                MERGE (hlb:HighLevelBatch {
                    sysId: $sysId,
                    activity_name: [$activity_name],
                    corr_batch_numbers: [$batch_number]
                })
                ON CREATE SET hlb.date = $eventDate
                RETURN hlb
                """
                try:
                    individual_node_result = tx.run(individual_node_query, sysId=sysId, activity_name=activity_name, batch_number=batch_number, eventDate=eventDate)
                    hlb_record = individual_node_result.single()
                except Exception as e:
                    print(f"Error creating individual node: {e}")
                    failed_count += 1
                    continue
                
                if hlb_record:
                    hlb_count += 1
                    created_nodes.add(tuple(sorted([batch_number])))
                    print(f"Created individual node: {hlb_record['hlb']}")
                else:
                    failed_count += 1
    except Exception as e:
        print(f"Error processing single batch result: {e}")


    print(f"Created or merged {hlb_count} HighLevelBatch nodes.")
    print(f"Failed to create {failed_count} HighLevelBatch nodes.")

def drop_duplicate_high_level_batches(tx):
    # Step 1: Identify duplicated nodes based on sysId, date, and corr_batch_numbers
    duplicate_query = """
            MATCH (hlb:HighLevelBatch)
            WITH hlb.sysId AS sysId, hlb.date AS date, hlb.corr_batch_numbers AS corr_batch_numbers, collect(hlb) AS nodes
            WHERE size(nodes) > 1
            RETURN sysId, date, corr_batch_numbers, nodes
    """

    try:
        duplicates_result = tx.run(duplicate_query)
    except Exception as e:
        print(f"Error running duplicate query: {e}")
        return
    
    try:
        for record in duplicates_result:
            nodes = record["nodes"]
            node_to_keep = nodes.pop(0)
            for node in nodes:
                delete_query = """
                MATCH (hlb:HighLevelBatch)
                WHERE id(hlb) = $node_id
                DETACH DELETE hlb
                """
                try:
                    tx.run(delete_query, node_id=node.id)
                    print(f"Deleted duplicate node with ID: {node.id}")
                except Exception as e:
                    print(f"Error deleting duplicate node with ID {node.id}: {e}")
    except Exception as e:
        print(f"Error processing duplicates result: {e}")

    print("Duplicate nodes removal completed.")

def connect_high_level_batch_to_batch_instances(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (hlb:HighLevelBatch) RETURN hlb",
            "UNWIND hlb.corr_batch_numbers AS id_val MATCH (batch:BatchInstance) WHERE id_val = batch.batch_number MERGE (batch)-[:CORR]->(hlb)",
            {batchSize:100})

        """)

def create_df_high_level_batch_edges(tx):       
    query = """
            MATCH (batch1:BatchInstance)-[r:DF_BATCH_RESOURCE]->(batch2:BatchInstance)
            MATCH (batch1)-[:CORR]->(hlb1:HighLevelBatch)
            MATCH (batch2)-[:CORR]->(hlb2:HighLevelBatch)
            WHERE hlb1 <> hlb2
            AND hlb1.sysId = hlb2.sysId
            AND hlb1.sysId = r.sysId
            AND hlb2.sysId = r.sysId
            WITH hlb1, hlb2, r
            ORDER BY r.order
            MERGE (hlb1)-[r2:DF_HIGH_LEVEL_BATCH {sysId: r.sysId}]->(hlb2)
            ON CREATE SET r2.count = r.count, r2.order = r.order
            ON MATCH SET r2.count = r2.count + r.count
    """
    result = tx.run(query)
    print(f"Created DF_HIGH_LEVEL_BATCH edges.")
    

def extend_and_cleanup_high_level_batches(tx):
    # Identify iterations
    loop_query = """
        MATCH (hlb1:HighLevelBatch)-[r:DF_HIGH_LEVEL_BATCH]->(hlb2:HighLevelBatch)
        MATCH (hlb2)-[r1:DF_HIGH_LEVEL_BATCH]->(hlb1)
        WHERE hlb1.sysId = hlb2.sysId and r.sysId=r1.sysId
        RETURN hlb1, hlb2, r, r1
    """

    try:
        loop_result = tx.run(loop_query)
    except Exception as e:
        print(f"Error running loop query: {e}")
        return

    try:
        for record in loop_result:
            hlb1 = record["hlb1"]
            hlb2 = record["hlb2"]
            r = record["r"]
            r1 = record["r1"]

            # Determine which node to extend and which to delete
            extend_node = None
            delete_node = None

            # Check which node has additional edges not part of the loop for the same resource
            additional_edges_query = """
            MATCH (hlb:HighLevelBatch)-[r:DF_HIGH_LEVEL_BATCH]->(other:HighLevelBatch)
            WHERE id(hlb) = $node_id AND id(other) <> $other_id
            AND hlb.sysId = other.sysId
            RETURN COUNT(r) AS additional_edges
            """

            try:
                hlb1_edges_result = tx.run(additional_edges_query, node_id=hlb1.id, other_id=hlb2.id)
                hlb2_edges_result = tx.run(additional_edges_query, node_id=hlb2.id, other_id=hlb1.id)
                hlb1_additional_edges = hlb1_edges_result.single()["additional_edges"]
                hlb2_additional_edges = hlb2_edges_result.single()["additional_edges"]

                if hlb1_additional_edges > 0:
                    extend_node = hlb1
                    delete_node = hlb2
                elif hlb2_additional_edges > 0:
                    extend_node = hlb2
                    delete_node = hlb1
                else:
                    # If both have no additional edges, choose the node with the smaller order attribute to extend
                    if r["order"] < r1["order"]:
                        extend_node = hlb1
                        delete_node = hlb2
                    else:
                        extend_node = hlb2
                        delete_node = hlb1

                if extend_node and delete_node:
                    # Extend the extend_node with data from delete_node
                    extend_query = """
                    MATCH (extend:HighLevelBatch)
                    WHERE id(extend) = $extend_id
                    MATCH (delete:HighLevelBatch)<-[:CORR]-(del_batch:BatchInstance)
                    WHERE id(delete) = $delete_id
                    MERGE (del_batch)-[:CORR]->(extend)
                    WITH extend, delete
                    // Extend properties of extend node
                    SET extend.corr_batch_numbers = apoc.coll.union(extend.corr_batch_numbers, delete.corr_batch_numbers),
                        extend.activity_name = extend.activity_name + apoc.coll.toSet(delete.activity_name)
                    """
        
                    try:
                        tx.run(extend_query, extend_id=extend_node.id, delete_id=delete_node.id)
                    except Exception as e:
                        print(f"Error extending node {extend_node.id} with data from node {delete_node.id}: {e}")
                        continue

                    # Delete the delete_node and its edges
                    delete_query = """
                    MATCH (delete:HighLevelBatch)
                    WHERE id(delete) = $delete_id
                    DETACH DELETE delete
                    """

                    try:
                        tx.run(delete_query, delete_id=delete_node.id)
                        print(f"Deleted node {delete_node.id}")
                    except Exception as e:
                        print(f"Error deleting node {delete_node.id}: {e}")
                        continue

            except Exception as e:
                print(f"Error determining additional edges: {e}")
                continue

    except Exception as e:
        print(f"Error processing loop result: {e}")

    print("Extended and cleaned up HighLevelBatch nodes in loops.")

def connect_high_level_batch_to_resource(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (hlb:HighLevelBatch) RETURN hlb",
            "UNWIND hlb.sysId AS id_val MATCH (u:Resource) WHERE id_val = u.sysId MERGE (u)-[:CORR]->(hlb)",
            {batchSize:100})

        """)
    
def connect_high_level_batch_to_events(tx):
        tx.run("""
            CALL apoc.periodic.iterate(
            "MATCH (hlb:HighLevelBatch) RETURN hlb",
            "UNWIND hlb.corr_batch_numbers AS id_val 
            MATCH (e:Event) WHERE id_val = e.batch 
            MATCH (u:Resource)<-[:CORR]-(e)
            WHERE hlb.sysId = u.sysId
            MERGE (e)-[:CORR]->(hlb)",
            {batchSize: 100})

        """)

def add_high_level_batches_attributes(tx):
    add_attributes = """
            MATCH (hlb:HighLevelBatch)
            WHERE hlb.corr_batch_numbers IS NOT NULL AND hlb.sysId IS NOT NULL
            WITH hlb, hlb.corr_batch_numbers AS batch_numbers, hlb.sysId AS sysId
            MATCH (u:Resource {sysId: sysId})<-[:CORR]-(e:Event)
            WHERE e.batch IN batch_numbers
            WITH hlb, MIN(e.timestamp) AS start_timestamp, MAX(e.timestamp) AS end_timestamp, COUNT(e) AS number_of_events
            SET hlb.start_timestamp = start_timestamp,
                hlb.end_timestamp = end_timestamp,
                hlb.number_of_events = number_of_events,
                hlb.activity_name = apoc.coll.toSet(hlb.activity_name)
            RETURN hlb
    """

    try:
        result = tx.run(add_attributes)
        for record in result:
            hlb = record["hlb"]
            print(f"Updated HighLevelBatch node {hlb['sysId']} with timestamps and event count.")
    except Exception as e:
        print(f"Error updating HighLevelBatch nodes: {e}") 

def set_work_together_attribute(tx):
    # Update nodes that satisfy the condition
    tx.run("""
        MATCH (n:HighLevelBatch)<-[:CORR]-(m:BatchInstance)
        WHERE n.sysId IN m.users
        WITH n, collect(m) AS batchInstances, [m IN collect(m) WHERE ANY(x IN m.users WHERE x <> n.sysId)] AS filteredBatchInstances
        WHERE size(filteredBatchInstances) > 0
        SET n.workTogether = true
        RETURN n
    """)
    
    # Update nodes that do not satisfy the condition
    tx.run("""
        MATCH (n:HighLevelBatch)
        WHERE n.workTogether IS NULL
        SET n.workTogether = false
        RETURN n
    """)


def main():
    with driver.session() as session:
        # Delete existing HighLevelBatch nodes and edges
        session.execute_write(delete_high_level_batches)
        # Create HighLevelBatch nodes
        session.execute_write(create_high_level_batches_v2)
        # Deletion of duplicated HighLevelBatch nodes
        session.execute_write(drop_duplicate_high_level_batches)
        print("Query to duplicates deletion executed.")
        # Connect HighLevelBatch to BatchInstance based on resource sysId
        session.execute_write(connect_high_level_batch_to_batch_instances)
        print("Query to connect BatchInstance to HighLevelBatch executed.")
        # Create DF edges 
        session.execute_write(create_df_high_level_batch_edges)
        print("Query to create DF_HIGH_LEVEL_BATCH edges executed.")
        # Extend and clean up HighLevelBatch nodes in iterative patterns
        session.execute_write(extend_and_cleanup_high_level_batches)
        print("Extended and cleaned up HighLevelBatch nodes in loops.")
        # Connect HighLevelBatch to Resource based on resource sysId
        session.execute_write(connect_high_level_batch_to_resource)
        print("Query to connect Resource to HighLevelBatch executed.")
        # Connect HighLevelBatch to Events based on resource sysId
        session.execute_write(connect_high_level_batch_to_events)
        print("Query to connect Events to HighLevelBatch executed.")
        # Add attributes to HighLevelBatch nodes
        session.execute_write(add_high_level_batches_attributes)
        print("Added attributes to HighLevelBatch nodes.")
        # Set the workTogether attribute
        session.execute_write(set_work_together_attribute)
        print("Set the workTogether attribute for HighLevelBatch nodes.")

if __name__ == "__main__":
    main()
