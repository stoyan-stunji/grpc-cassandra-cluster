FROM openjdk:11-jdk-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y git maven \
 && git clone https://github.com/apache/cassandra.git .

RUN mvn -T1C -DskipTests clean package

FROM openjdk:11-jre-slim-bullseye

RUN useradd -m -d /home/cassandra cassandra

WORKDIR /opt/cassandra

COPY --from=builder /build/build/apache-cassandra-*/ /opt/cassandra/

EXPOSE 9042

VOLUME /var/lib/cassandra

USER cassandra

CMD ["bin/cassandra", "-f"]
