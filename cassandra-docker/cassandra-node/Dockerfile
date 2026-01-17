# Builder stage
FROM openjdk:11-jdk-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        maven \
    && rm -rf /var/lib/apt/lists/*

ARG CASSANDRA_REF=cassandra-2.0

RUN git clone --depth 1 --branch ${CASSANDRA_REF} \
    https://github.com/stoyan-stunji/virt1.git /tmp/src

WORKDIR /tmp/src

RUN mvn -B -DskipTests dependency:go-offline

# Source layer
WORKDIR /build
RUN cp -R /tmp/src/. .

RUN mvn -DskipTests clean package

# Runtime stage
FROM openjdk:11-jre-slim-bullseye

RUN useradd -r -m -d /home/cassandra cassandra

WORKDIR /opt/cassandra

COPY --from=builder /build/build/apache-cassandra-*/ /opt/cassandra/

RUN rm -rf doc tools stress *.md *.txt

RUN chown -R cassandra:cassandra /opt/cassandra

RUN mv conf/cassandra.yaml conf/cassandra.yaml.template

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7000 7001 7199 9042

VOLUME /var/lib/cassandra

USER cassandra

ENTRYPOINT ["docker-entrypoint.sh"]
