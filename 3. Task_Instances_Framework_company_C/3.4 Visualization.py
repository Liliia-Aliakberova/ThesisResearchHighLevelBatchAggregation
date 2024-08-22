"""
Current code is dedicated to the final part of Applying Task Instance Framework 
over Company C’s EKG following methodology proposed by 
Klijn, Mannhardt, and Fahland in the work "Aggregating event
knowledge graphs for task analysis." The link to the research paper
(https://link.springer.com/chapter/10.1007/978-3-031-27815-0_36)" 
"""


from neo4j import GraphDatabase
from graphviz import Digraph
from dotenv import load_dotenv
import os

# Define Neo4j connection details
load_dotenv()
uri = os.getenv('NEO4J_URI')
username = os.getenv('NEO4J_USER')
password = os.getenv('NEO4J_PASSWORD')

# Function to execute the Neo4j query
def execute_query(query):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            result = session.run(query)
            return result.data()

# Define the Neo4j query
neo4j_query = """
MATCH (tc1:TaskCluster)<-[:TI_OBSERVED]-(ti1:TaskInstance)-[:CORR]->(m:Resource) 
WITH ti1, m, tc1
MATCH (tc2:TaskCluster)<-[:TI_OBSERVED]-(ti2:TaskInstance)-[:CORR]->(m:Resource)
WHERE m.sysId="LI"
AND date(ti1.start_time) = date('2022-01-01')  
AND date(ti2.start_time) = date('2022-01-01') 
WITH ti1, m, ti2, tc1, tc2
MATCH (ti1)-[df:DF_TI {EntityType: 'resource'}]->(ti2)
RETURN  DISTINCT ti1.cluster AS source_cluster, ti1.clusterID AS source_clusterID,
        m.sysId AS sysId, ti2.cluster AS target_cluster, ti2.clusterID AS target_clusterID,
        count(df) as abs_freq
"""

# Execute the Neo4j query
query_result = execute_query(neo4j_query)

# Create a directed graph
dot = Digraph()

# Add nodes and edges to the graph based on the query result
for row in query_result:
    source_cluster = str(row["source_cluster"])
    target_cluster = str(row["target_cluster"])
    dot.node(source_cluster, label=source_cluster)
    dot.node(target_cluster, label=target_cluster)
    dot.edge(source_cluster, target_cluster, label=str(row["abs_freq"]))

# Render and display the graph using Graphviz
dot.render('dfg_graph', format='png', cleanup=True)
dot.view()
