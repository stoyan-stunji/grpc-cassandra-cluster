This project was developed as part of the course "Intro to Virtualization Technologies" at Sofia University, FMI. 
It demonstrates how containerization and distributed databases can work together to create scalable, 
resilient, and efficient systems using Docker and Apache Cassandra.

### Why Apache Cassandra?

Apache Cassandra is a highly scalable, distributed NoSQL database designed to handle large amounts of data across 
multiple servers without a single point of failure. It provides high availability, horizontal scalability, and 
fault tolerance.

Cassandra’s architecture aligns naturally with containerization: each node is equal and can start or stop 
independently, which fits Docker’s stateless and replaceable container model. Just as Docker scales by 
adding containers, Cassandra scales by adding nodes. In a containerized setup, if a node (container) fails, 
other replicas continue serving traffic, or a new container can join the cluster seamlessly.

Data in Cassandra is distributed using consistent hashing, so no single container stores all the data. This 
eliminates the need for shared storage between containers, making Docker volume isolation straightforward: each 
container manages its own persistent volume. Cassandra also provides official Docker images and environment-based 
configuration, simplifying deployment.

### Why Dockerize?

Dockerization packages an application, its runtime, dependencies, and configuration into a Docker 
image that runs as a container. In this project, all major components are containerized:
- Three Cassandra nodes
- gRPC service
- NGINX gateway
- Two HTTP services for testing database requests

Orchestration is handled using Docker Compose, which ensures reproducible environments, service isolation, easy 
scaling, and portable deployment.

### Cassandra Nodes

Each node uses the “cassandra:4.1” Docker image. A dedicated named volume is attached to every node to persist data, 
ensuring it is not lost when the container is stopped. Configuration is managed through environment variables, which 
centralizes and simplifies the setup. Additionally, each node includes a “depends_on” condition, requiring the previous
node to be healthy before the next one starts, preventing errors when joining the cluster.

### NGINX Container

This container acts as a reverse proxy to the Cassandra user port. It is positioned in front of the internal services 
and forwards incoming requests to the appropriate backend servers.
```yaml
Client → NGINX → Cluster
````
This approach hides the internal architecture, centralizes incoming traffic, enables load balancing, and provides SSL 
termination. Only NGINX handles the SSL protocol, meaning the encryption workload is placed on a single server rather 
than on all microservices. This reduces overall CPU usage across the system. When the SSL certificate expires, 
it needs to be renewed in only one place, which makes maintenance easier and more efficient.
The container starts only after all three Cassandra nodes are healthy. It exposes the Cassandra client port – 9042, 
and 8080, 8081, 8802 – web interfaces used for manual testing.
