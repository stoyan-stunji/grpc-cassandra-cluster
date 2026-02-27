This project was developed as part of the course "Intro to Virtualization Technologies" at Sofia University, FMI. 
It demonstrates how containerization and distributed databases can work together to create scalable, 
resilient and efficient systems using Docker and Apache Cassandra.

### Why Apache Cassandra?
Apache Cassandra is a highly scalable, distributed NoSQL database designed to handle large amounts of data across 
multiple servers without a single point of failure. It provides high availability, horizontal scalability and 
fault tolerance.

Cassandra’s architecture aligns naturally with containerization: each node is equal and can start or stop 
independently, which fits Docker’s stateless and replaceable container model. Just as Docker scales by 
adding containers, Cassandra scales by adding nodes. In a containerized setup, if a node fails, 
other replicas continue serving traffic or a new container can be added to the cluster seamlessly.

Data in Cassandra is distributed using consistent hashing, so no single container stores all the data. This 
eliminates the need for shared storage between containers, making Docker volume isolation straightforward: each 
container manages its own persistent volume. Cassandra also provides official Docker images and environment-based 
configuration, simplifying deployment.

### Why Dockerize?
Dockerization packages an application, its runtime, dependencies and configuration into a Docker 
image that runs as a container. In this project all major components are containerized:
- Three Cassandra nodes;
- gRPC service;
- NGINX gateway;
- Two HTTP services for testing database requests.

Orchestration is handled using Docker Compose, which ensures reproducible environments, service isolation, easy 
scaling and portable deployment.

### Cassandra Nodes
Each node uses the `cassandra:4.1` Docker image. A dedicated named volume is attached to every node to persist data, 
ensuring it is not lost when the container is stopped. Configuration is managed through environment variables, which 
centralizes the setup. Additionally, each node includes a `depends_on` condition, requiring the previous
node to be healthy before the next one starts, preventing errors when joining the cluster.

### NGINX Container
This container acts as a reverse proxy to the Cassandra user port. It is positioned in front of the internal services 
and forwards incoming requests to the appropriate backend servers:
```yaml
Client → NGINX → Cluster
````
This approach hides the internal architecture, centralizes incoming traffic, enables load balancing and provides SSL 
termination. Only NGINX handles the SSL protocol, meaning the encryption workload is placed on a single server rather 
than on all microservices. This reduces overall CPU usage across the system. When the SSL certificate expires, 
it needs to be renewed at only one place, which makes maintenance easier and more efficient. The container starts only 
after all three Cassandra nodes are healthy. It exposes the Cassandra client port – `9042`, 
and `8080`, `8081`, `8802` – web interfaces used for manual testing.

### Custom Services
These services are built from their own Docker images, for example from the directory `./service_a`. They are connected 
within a Docker network called `client-net`. They depend on Cassandra and start only after the entire Cassandra cluster 
is fully initialized and ready.

### Networks
There are two separate networks in the setup. The `cluster-net` is used for communication between the Cassandra 
nodes. In contrast, `client-net` is used by external clients, such as NGINX and service_a/service_b. This network isolation 
separates internal cluster communication from external access. As a result, internal traffic between Cassandra nodes remains 
isolated, while external requests are routed through the reverse proxy before reaching the cluster, improving organization, 
security and traffic management.

### Remote Procedure Call
The RPC protocol allows a function to be called on a remote machine as if it were a local function. Instead of implementing 
complex network logic such as sockets, HTTP requests and manual data handling, the developer simply calls a function and 
the RPC system takes care of the communication. It automatically serializes the parameters, sends them over the network, executes 
the function on the remote server and returns the result. From the developer’s perspective, it looks like a normal function
call, but in reality it is network communication.

### gRPC
gRPC is an RPC framework that uses HTTP/2. HTTP/2 supports multiplexing, meaning multiple requests can be sent over a single 
connection simultaneously, improving performance and efficiency.

Why gRPC instead of REST?
Unlike REST, which typically uses JSON, gRPC relies on binary protocols. This results in smaller message 
sizes, reduced network traffic and lower latency. In this case, gRPC is used for communication between microservices. 
There is no need for browser compatibility or human-readable JSON. Instead, the priority is speed, efficiency, 
and type safety. With gRPC the service interface is defined in a .proto file. This file clearly specifies input and output 
types and the corresponding code is automatically generated. This reduces runtime errors and ensures stricter type validation.
gRPC is a logical choice for an API in front of the database, because it is designed to handle high traffic, many 
concurrent requests and low-latency communication efficiently.
```yaml
Client → NGINX → gRPC API → Cluster
```

### Optimization
#### No Bloated Images
No bloated images were used in this setup. Such images typically include packages that the application does not need, along 
with debug tools, full build environments like compilers, SDKs and etc., unnecessary dependencies and temporary files. All of this significantly 
increases the image size, leading to slower downloads and startup times, as well as higher disk space usage. Larger images also 
reduce security: more installed packages mean a bigger attack surface. In addition, maintenance becomes more difficult due to 
the increased number of dependencies and the added complexity when debugging. The solution is to use lightweight base images 
such as `slim` variants – for example `python:3.11-slim`, `openjdk:11-jdk-slim` and etc. These images contain only the 
essential components required to run the application, resulting in smaller size, improved security and easier maintenance.

#### Non-root
Each container runs as a non-root user, granting permissions only where necessary. Running containers as root creates a 
security vulnerability: if an attacker gains access, they obtain root privileges inside the container. This can lead to 
unauthorized access to host files, container escapes and the ability to alter other containers.

#### Minimized Number Of Layers
The number of layers has been minimized by combining multiple commands into a single layer. Each `RUN`, `COPY`, `ADD` and etc. 
creates a new layer, which is cached and stacked on top of previous layers. Even if a file is deleted in a later layer, 
it remains in the cache of the earlier layer. By combining commands into fewer layers, Docker can cache logically connected 
steps more efficiently, reducing unnecessary cache invalidation and keeping the image smaller. 

#### Clean-up The Unnecessary
All unnecessary files – such as `.doc`, `.md`, `.stress` and etc., have been removed, since they are not needed at runtime. 
Keeping them would waste space and increase the time required to push or pull the image. The production image is separate 
from the development environment and should include only what is essential for runtime execution. In addition, a `.dockerignore` 
files have been created to specify which files and directories should be excluded from the build context sent to the Docker 
daemon. This reduces unnecessary data transfer, speeds up the build process and keeps the image lean.

#### Leveraged Layer Caching
The project leverages layer-level caching for improved build efficiency. Following the principle of separation of concerns 
enhances cache effectiveness: dependencies, which change infrequently, are handled in a separate layer from source compilation, 
which changes frequently. When using `git clone` this approach prevents unnecessary cache invalidation – dependencies are cached 
in a layer before the source code and only the latest commit is cloned, minimizing cache misses and speeding up builds.

#### Environmental Variables
To prevent environment leakage and avoid hardcoded configurations, environment variables are used. Hardcoding configuration 
embeds it in the image, leaving sensitive data in the layers when the image is rebuilt i.e. security risk. The solution is to 
inject configuration at runtime rather than at build time, keeping sensitive information out of the image and improving security.

#### The Advantge Of Dockes's Internal DNS
Docker IPs are dynamic, meaning a container’s IP can change when it is restarted. By using container names as seeds, Docker’s 
internal DNS automatically resolves the names, ensuring consistent connectivity. This also prevents mismatches between the 
listen address and broadcast address, which can otherwise cause issues in cluster communication.

#### (Im)proper Logging
Writing logs to files inside a container is considered bad practice, because containers have internal log rotation and logs 
are lost when the container is restarted. To maintain stateless containers, logs should be sent to `STDOUT` or `STDERR`, which 
are captured and managed by the platform, ensuring persistence and easier monitoring.

#### Dependency Ordering + Health Checks
By default, Docker only knows if the process is working and doesn’t know if its actually ready. This can lead to race 
conditions, crash loops and instability when restarting. By using `depends_on` and health checks we can guarantee 
self-healing – this way when the dependency is lost and becomes unhealty, the orchestrator can restart the container 
or turn off the traffic to it. 

#### Multistage Builds
Multistage Builds separate the build and runtime environments. By definition, a multistage build uses more than one `FROM` 
instruction: a build stage for compiling, building and installing dependencies, and a runtime stage containing only what 
is necessary to run the application. Using multistage builds provides several benefits: smaller image sizes, faster push 
and pull times, improved security, a clean separation of concerns and more efficient caching.

### Conclusion
This project demonstrates how Docker and Apache Cassandra can be combined to create a scalable, resilient and efficient
distributed system. By leveraging containerization, lightweight images, network isolation and gRPC communication, the 
setup ensures high availability, maintainability and performance, providing a practical example of modern virtualization
and microservice architecture principles.

### Literature
- [The Twelve-Factor App](https://www.12factor.net/)
- [Apache Cassandra Documentation](https://cassandra.apache.org/_/index.html)
- [Microservice Demo](https://github.com/Joker666/microservice-demo)
- [The Popek and Goldberg Theorem (1974)](https://www.cs.cornell.edu/courses/cs6411/2018sp/papers/popek-goldberg.pdf)
- [Virtual Machines - James E. Smith & Ravi Nair](http://ndl.ethernet.edu.et/bitstream/123456789/42492/1/14.pdf)
