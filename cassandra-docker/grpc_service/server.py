import os
import grpc
from concurrent import futures
from cassandra.cluster import Cluster

import cluster_pb2
import cluster_pb2_grpc

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cass_nginx")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

class ClusterService(cluster_pb2_grpc.ClusterServiceServicer):

    def GetClusterInfo(self, request, context):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()

            name_row = session.execute("SELECT cluster_name FROM system.local").one()

            ks_rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")

            cluster.shutdown()

            return cluster_pb2.ClusterResponse(
                cluster_name=name_row.cluster_name if name_row else "unknown",
                keyspace_count=len(list(ks_rows))
            )

        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return cluster_pb2.ClusterResponse()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    cluster_pb2_grpc.add_ClusterServiceServicer_to_server(ClusterService(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    print(f"gRPC server listening on 0.0.0.0:{GRPC_PORT}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
