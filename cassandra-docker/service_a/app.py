from flask import Flask
import os
from datetime import datetime
import time
from grpc_client import get_cluster_info  

app = Flask(__name__)
HOSTNAME = os.environ.get('HOSTNAME', 'unknown')

@app.route("/")
def root():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    info = get_cluster_info()
    response = f"[{now}] SERVICE_A! Container: {HOSTNAME}, Cluster={info.cluster_name}, Keyspaces={info.keyspace_count}"
    print(response)
    return response

@app.route("/slow")
def slow():
    time.sleep(3)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    info = get_cluster_info()
    response = f"[{now}] SERVICE_A (slow)! Container: {HOSTNAME}, Cluster={info.cluster_name}, Keyspaces={info.keyspace_count}"
    print(response)
    return response

if __name__ == "__main__":
    print(f"Starting service_a in container {HOSTNAME}...")
    app.run(host="0.0.0.0", port=8000, threaded=True)
