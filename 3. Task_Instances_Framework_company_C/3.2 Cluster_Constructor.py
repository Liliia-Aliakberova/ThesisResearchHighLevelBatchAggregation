"""
Current code is dedicated to the second part of Applying Task Instance Framework 
over Company C’s EKG following methodology proposed by 
Klijn, Mannhardt, and Fahland in the work "Aggregating event
knowledge graphs for task analysis." The link to the research paper
(https://link.springer.com/chapter/10.1007/978-3-031-27815-0_36)" 
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Define Neo4j connection details
load_dotenv()
uri = os.getenv('NEO4J_URI')
username = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASSWORD')

# Define queries
queries = [
    """
    // Query A
    CALL apoc.periodic.iterate( 
    "MATCH (ti:TaskInstance) WHERE ti.cluster IS NOT NULL  
    RETURN DISTINCT ti.cluster AS cluster, count(*) AS cluster_count", 
    "WITH cluster, cluster_count 
    MERGE (tc:TaskCluster {Name:cluster, count:cluster_count})", 
    {batchSize:100}) 
    """,
    """
    // Query B
    CALL apoc.periodic.iterate( 
    "MATCH (tc:TaskCluster) 
    MATCH (ti:TaskInstance) WHERE ti.cluster = tc.Name 
    RETURN tc, ti", 
    "WITH tc, ti 
    CREATE (ti)-[:TI_OBSERVED]->(tc)", 
    {batchSize:100}) 
    """,
    """
    // Query C
    MATCH (tc1:TaskCluster)<-[:TI_OBSERVED]-(ti1:TaskInstance)-[df:DF_TI]->(ti2:TaskInstance)-[:TI_OBSERVED]->(tc2:TaskCluster) 
    MATCH (ti1)-[:CORR]->(user:Resource)<-[:CORR]-(ti2) 
    WHERE user:Resource and df.EntityType = "case" 
    WITH user, tc1, count(df) AS df_freq, tc2 
    MERGE (tc1)-[rel2:DF_TC{EntityType:"resource"}]->(tc2) ON CREATE SET rel2.count=df_freq 
    """,

    """
    // Query D
    CREATE (:TaskCluster {Name:"start"})
    """,
    """
    // Query E
    CREATE (:TaskCluster {Name:"end"}) 
    """,
    """
    // Query F
    MATCH (tc:TaskCluster) WHERE NOT (:TaskCluster)-[:DF_TC {EntityType:"case"}]->(tc) 
    AND NOT tc.Name IN ["start", "end"] 
    WITH tc, tc.count AS count 
    MATCH (start:TaskCluster {Name:"start"}) 
    WITH start, tc, count 
    MERGE (start)-[df:DF_TC {EntityType:"case", count:count}]->(tc) 
    """,
    """
    // Query G
    MATCH (tc:TaskCluster) WHERE NOT (tc)-[:DF_TC {EntityType:"case"}]->(:TaskCluster)  
    AND NOT tc.Name IN ["start", "end"]  
    WITH tc, tc.count AS count  
    MATCH (end:TaskCluster {Name:"end"})  
    WITH end, tc, count  
    MERGE (tc)-[df:DF_TC {EntityType:"case", count:count}]->(end)
    """,
    """
    // Query H
    MATCH (ti0:TaskInstance)-[df:DF_TI {EntityType:"resource"}]->(ti1:TaskInstance) 
    WHERE NOT date(ti0.end_time) = date(ti1.start_time) AND ti1.cluster IS NOT NULL 
    WITH DISTINCT ti1.cluster AS cluster, count(*) AS count 
    MATCH (tc:TaskCluster {Name:cluster}) 
    WITH tc, count 
    MATCH (start:TaskCluster {Name:"start"}) 
    WITH tc, start, count 
    MERGE (start)-[df:DF_TC {EntityType:"resource", count:count}]->(tc) 
    """,
    """
    // Query I
    MATCH (ti0:TaskInstance)-[df:DF_TI {EntityType:"resource"}]->(ti1:TaskInstance) WHERE NOT date(ti0.end_time) = date(ti1.start_time) and ti0.cluster IS NOT NULL 
    WITH DISTINCT ti0.cluster AS cluster, count(*) AS count 
    MATCH (tc:TaskCluster {Name:cluster}) 
    WITH tc, count 
    MATCH (end:TaskCluster {Name:"end"}) 
    WITH tc, end, count 
    MERGE (tc)-[df:DF_TC {EntityType:"resource", count:count}]->(end) 
    """
]

# Function to execute queries
def cluster_queries(queries):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            for query in queries:
                session.run(query)

# Execute queries
cluster_queries(queries)
