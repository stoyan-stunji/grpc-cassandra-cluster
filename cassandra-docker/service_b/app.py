from flask import Flask
import os
from datetime import datetime
from cassandra.cluster import Cluster

app = Flask(__name__)
HOSTNAME = os.environ.get('HOSTNAME', 'unknown')

CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cass_nginx')
CASSANDRA_PORT = int(os.environ.get('CASSANDRA_PORT', 9042))

def query_cassandra():
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        # Fetch number of keyspaces as a demo
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspaces = [r.keyspace_name for r in rows]
        cluster.shutdown()
        return f"{len(keyspaces)} keyspaces: {', '.join(keyspaces)}"
    except Exception as e:
        return f"ERROR: {e}"

@app.route("/")
def root():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ks_info = query_cassandra()
    response = f"[{now}] Hello from service_b! (container: {HOSTNAME}), Cassandra Keyspaces: {ks_info}"
    print(response)
    return response

@app.route("/slow")
def slow():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ks_info = query_cassandra()
    response = f"[{now}] Slow response from service_b (container: {HOSTNAME}), Cassandra Keyspaces: {ks_info}"
    print(response)
    return response

if __name__ == "__main__":
    print(f"Starting service_b in container {HOSTNAME}...")
    app.run(host="0.0.0.0", port=8000)
