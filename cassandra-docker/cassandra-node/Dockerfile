# builder
FROM openjdk:11-jdk-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        maven \
    && rm -rf /var/lib/apt/lists/*

ARG CASSANDRA_REF=cassandra-2.0
RUN git clone --depth 1 --branch ${CASSANDRA_REF} \
    https://github.com/stoyan-stunji/virt1.git .

RUN mvn -DskipTests dependency:go-offline

RUN mvn -DskipTests clean package

# runtime
FROM openjdk:11-jre-slim-bullseye

RUN useradd -r -m -d /home/cassandra cassandra

WORKDIR /opt/cassandra

COPY --from=builder /build/build/apache-cassandra-*/ /opt/cassandra/

RUN rm -rf doc tools stress *.md *.txt

RUN chown -R cassandra:cassandra /opt/cassandra

ENV CASSANDRA_CLUSTER_NAME="Test Cluster" \
    CASSANDRA_LISTEN_ADDRESS=auto \
    CASSANDRA_RPC_ADDRESS=0.0.0.0 \
    CASSANDRA_SEEDS=127.0.0.1 \
    CASSANDRA_HEAP_SIZE=1024M

RUN mv conf/cassandra.yaml conf/cassandra.yaml.template

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
 
EXPOSE 7000 7001 7199 9042

VOLUME /var/lib/cassandra

USER cassandra

ENTRYPOINT ["docker-entrypoint.sh"]
