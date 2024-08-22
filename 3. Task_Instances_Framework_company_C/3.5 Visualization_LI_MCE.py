"""
Current code is dedicated to provide an example" 
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

# Define the Neo4j query for LI and MCE
combined_query = """
MATCH (tc1:TaskCluster)<-[:TI_OBSERVED]-(ti1:TaskInstance)-[:CORR]->(m:Resource) 
WITH ti1, m, tc1
MATCH (tc2:TaskCluster)<-[:TI_OBSERVED]-(ti2:TaskInstance)-[:CORR]->(m:Resource)
WHERE m.sysId IN ["LI", "MCE"]
AND date(ti1.start_time) = date('2022-01-01')  
AND date(ti2.start_time) = date('2022-01-01') 
WITH ti1, m, ti2, tc1, tc2
MATCH (ti1)-[df:DF_TI {EntityType: 'resource'}]->(ti2)
RETURN  DISTINCT ti1.cluster AS source_cluster, ti1.clusterID AS source_clusterID,
        m.sysId AS sysId, ti2.cluster AS target_cluster, ti2.clusterID AS target_clusterID,
        count(df) as abs_freq
"""

# Execute the Neo4j query
result = execute_query(combined_query)

# Create a directed graph with Graphviz
dot = Digraph()

# Dictionary to store the total frequency of each node
total_freq = {}

# Add nodes and edges
for row in result:
    source_cluster = str(row["source_cluster"])
    target_cluster = str(row["target_cluster"])
    # Use a different color for LI and MCE
    color = "lightseagreen" if row["sysId"] == "LI" else "red"
    # Update total frequency for source and target clusters
    dot.edge(source_cluster, target_cluster, label=str(row["abs_freq"]), color=color)

# Render and display the graph using Graphviz
dot.render('combined_dfg_graph_with_frequency', format='png', cleanup=True)
dot.view()
