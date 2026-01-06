from cassandra.cluster import Cluster
import threading
import requests

NUM_CONNECTIONS = 10
CASSANDRA_HOST = '127.0.0.1'
CASSANDRA_PORT = 9042
HTTP_URL = 'http://localhost:8080/'

cassandra_results = []
http_results = []

def query_cassandra(index):
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()

        row = session.execute("SELECT host_id, broadcast_address FROM system.local").one()
        node_id = row.host_id
        node_ip = row.broadcast_address

        cassandra_results.append((index, node_id, node_ip))
        cluster.shutdown()
    except Exception as e:
        cassandra_results.append((index, f"ERROR: {e}"))

def query_http(index):
    try:
        r = requests.get(HTTP_URL, timeout=5)
        http_results.append((index, r.text.strip()))
    except Exception as e:
        http_results.append((index, f"ERROR: {e}"))

threads = []

for i in range(NUM_CONNECTIONS):
    t = threading.Thread(target=query_cassandra, args=(i+1,))
    t.start()
    threads.append(t)

for i in range(NUM_CONNECTIONS):
    t = threading.Thread(target=query_http, args=(i+1,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print("\nCassandra Load Balancing Test Results (via NGINX):")
for r in cassandra_results:
    if len(r) == 3:
        print(f"Connection {r[0]}: handled by node with host_id={r[1]} ip={r[2]}")
    else:
        print(f"Connection {r[0]}: {r[1]}")

print("\nHTTP Load Balancing Test Results (service_a & service_b via NGINX):")
for r in http_results:
    print(f"Request {r[0]}: handled by {r[1]}")
