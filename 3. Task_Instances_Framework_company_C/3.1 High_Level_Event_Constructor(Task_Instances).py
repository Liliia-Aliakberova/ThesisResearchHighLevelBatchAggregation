"""
Current code is dedicated to the first part of Applying Task Instance Framework 
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
    "MATCH (e1:Event)-[:DF_RESOURCE]->(e2:Event) 
    MATCH (e1)-[:DF_RUN]->(e2) 
    RETURN e1,e2",
    "WITH e1,e2 
    MERGE (e1)-[:DF_JOINT]->(e2)",
    {batchSize:100})
    """,
    """
    // Query B
    CALL apoc.periodic.iterate(
    "CALL {
    MATCH (e1:Event)-[:DF_JOINT]->() WHERE NOT ()-[:DF_JOINT]->(e1) 
    MATCH ()-[:DF_JOINT]->(e2:Event) WHERE NOT (e2)-[:DF_JOINT]->() 
    MATCH p=(e1)-[:DF_JOINT*]->(e2) 
    RETURN p, e1, e2, NULL as caseID, NULL as resource 
    UNION 
    MATCH (ur:Run)<-[:CORR]-(e:Event)-[:CORR]->(u:Resource) WHERE u.sysId IS NOT NULL 
    AND NOT ()-[:DF_JOINT]->(e) AND NOT (e)-[:DF_JOINT]->() 
    MATCH p=(e) RETURN p, e AS e1, e AS e2, ur.runId as caseID, u.sysId as resource 
    } 
    RETURN [event in nodes(p) | event.activity] AS path,  
    resource AS resource, caseID AS caseID,  
    nodes(p) AS events, e1.timestamp AS start_time, e2.timestamp AS end_time",
    "WITH path, resource, caseID, events, start_time, end_time 
    CREATE (ti:TaskInstance {path:path, rID:resource, cID:caseID, start_time:start_time, end_time:end_time, r_count: 1, c_count: 1}) 
    WITH ti, events 
    UNWIND events AS e 
    CREATE (e)<-[:CONTAINS]-(ti)",
    {batchSize:100})
    """,
    """
    // Query C
    CALL apoc.periodic.iterate(
    "MATCH (ti:TaskInstance)
    MATCH (n:Run) WHERE ti.cID = n.runId
    RETURN ti,n",
    "WITH ti,n
    CREATE (ti)-[:CORR]->(n)",
    {batchSize:100})
    """,

    """
    // Query C 2 part
    CALL apoc.periodic.iterate(
    "MATCH (ti:TaskInstance)
    MATCH (n:Resource) WHERE  ti.rID = n.sysId 
    RETURN ti,n",
    "WITH ti,n
    CREATE (ti)-[:CORR]->(n)",
    {batchSize:100})
    """,
    """
    // Query D
    CALL apoc.periodic.iterate(
    "MATCH (n:Run)  
    MATCH (ti:TaskInstance)-[:CORR]->(n) 
    WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti) 
    WITH n, COLLECT (nodes) as nodeList 
    UNWIND range(0, size(nodeList)-2) AS i 
    RETURN n, nodeList[i] as ti_first, nodeList[i+1] as ti_second",
    "WITH n,ti_first,ti_second 
    MERGE (ti_first)-[df:DF_TI{EntityType:'case'}]->(ti_second)",
    {batchSize:100})
    """,
    """
    // Query D 2 part
    CALL apoc.periodic.iterate(
    "MATCH (n:Resource)   
    MATCH (ti:TaskInstance)-[:CORR]->(n) 
    WITH n, ti AS nodes ORDER BY ti.start_time, ID(ti) 
    WITH n, COLLECT (nodes) as nodeList 
    UNWIND range(0, size(nodeList)-2) AS i 
    RETURN n, nodeList[i] as ti_first, nodeList[i+1] as ti_second",
    "WITH n,ti_first,ti_second 
    MERGE (ti_first)-[df:DF_TI{EntityType:'resource'}]->(ti_second)",
    {batchSize:100})
    """,
    """
    // Query E
    CALL apoc.periodic.iterate(
    "MATCH (ti:TaskInstance) 
    WITH DISTINCT ti.path AS path, count(*) AS count 
    ORDER BY count DESC 
    WITH collect(path) as paths 
    UNWIND range(0, size(paths)-1) as pos 
    WITH paths[pos] AS path, pos+1 AS rank 
    MATCH (ti:TaskInstance) WHERE ti.path = path 
    RETURN ti, rank",
    "WITH ti, rank 
    SET ti.ID = rank",
    {batchSize:100})
    """
]

# Function to execute queries
def execute_queries(queries):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            for query in queries:
                session.run(query)

# Execute queries
execute_queries(queries)
