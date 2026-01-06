from flask import Flask
import os
from datetime import datetime
from cassandra.cluster import Cluster

app = Flask(__name__)
HOSTNAME = os.environ.get('HOSTNAME', 'unknown')

# Cassandra config
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cass_nginx')  # NGINX TCP LB
CASSANDRA_PORT = int(os.environ.get('CASSANDRA_PORT', 9042))

def query_cassandra():
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        # Simple query: fetch Cassandra cluster name from system.local
        row = session.execute("SELECT cluster_name FROM system.local").one()
        cluster_name = row.cluster_name if row else "unknown"
        cluster.shutdown()
        return cluster_name
    except Exception as e:
        return f"ERROR: {e}"

@app.route("/")
def root():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cluster_name = query_cassandra()
    response = f"[{now}] Hello from service_a! (container: {HOSTNAME}), Cassandra Cluster: {cluster_name}"
    print(response)
    return response

@app.route("/slow")
def slow():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cluster_name = query_cassandra()
    response = f"[{now}] Slow response from service_a (container: {HOSTNAME}), Cassandra Cluster: {cluster_name}"
    print(response)
    return response

if __name__ == "__main__":
    print(f"Starting service_a in container {HOSTNAME}...")
    app.run(host="0.0.0.0", port=8000)
