"""
Current code is dedicated to the third part of Applying Task Instance Framework 
over Company C’s EKG following methodology proposed by 
Klijn, Mannhardt, and Fahland in the work "Aggregating event
knowledge graphs for task analysis." The link to the research paper
(https://link.springer.com/chapter/10.1007/978-3-031-27815-0_36)" 
"""


from neo4j import GraphDatabase
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

class TaskAggregator:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def filter_task_instances(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (ti:TaskInstance) 
                WITH ti.path AS path, ti.ID AS ID, COUNT(*) AS frequency  
                WHERE frequency >= 16
                RETURN path AS path, ID AS ID
            """)
            return [(record["ID"], record["path"]) for record in result]

    def perform_agglomerative_clustering(self, task_instances):
        paths = [' '.join(path) for _, path in task_instances]

        vectorizer = TfidfVectorizer()
        X = vectorizer.fit_transform(paths).toarray()

        silhouette_scores = []
        max_silhouette_score = -1
        best_num_clusters = 0

        for num_clusters in range(2, len(task_instances)):
            agglomerative = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
            clusters = agglomerative.fit_predict(X)
            silhouette_avg = silhouette_score(X, clusters)
            silhouette_scores.append(silhouette_avg)

            if silhouette_avg > max_silhouette_score:
                max_silhouette_score = silhouette_avg
                best_num_clusters = num_clusters

        print(f"Best number of clusters: {best_num_clusters}")
        print(f"Max Silhouette Score: {max_silhouette_score}")

        agglomerative = AgglomerativeClustering(n_clusters=best_num_clusters, affinity='euclidean', linkage='ward')
        clusters = agglomerative.fit_predict(X)
        
        unique_labels = np.unique(clusters)
        if len(unique_labels) < 2:
            raise ValueError("Number of unique labels is less than 2, cannot compute silhouette score.")
        
        label_map = {old_label: new_label for new_label, old_label in enumerate(unique_labels)}
        clusters_mapped = np.array([label_map[label] for label in clusters])

        return clusters_mapped

    def assign_cluster_labels(self, task_instances, cluster_labels):
        with self.driver.session() as session:
            for i, (task_id, path) in enumerate(task_instances):
                cluster_label = f"Cluster_{cluster_labels[i]}"
                session.run("""
                    MATCH (ti:TaskInstance {ID: $task_id})
                    SET ti.cluster = $path,
                        ti.clusterID = $cluster_label
                """, task_id=task_id, path=path, cluster_label=cluster_label)

    def main(self):
        task_instances = self.filter_task_instances()
        clusters = self.perform_agglomerative_clustering(task_instances)
        self.assign_cluster_labels(task_instances, clusters)

    
if __name__ == "__main__":
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    aggregator = TaskAggregator(uri, username, password)
    aggregator.main()


