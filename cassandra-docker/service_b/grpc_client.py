import grpc
import cluster_pb2
import cluster_pb2_grpc

def get_cluster_info():
    with grpc.insecure_channel("grpc_api:50051") as channel:
        stub = cluster_pb2_grpc.ClusterServiceStub(channel)
        return stub.GetClusterInfo(cluster_pb2.ClusterRequest())
