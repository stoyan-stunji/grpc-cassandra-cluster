#!/bin/sh
set -e

CONFIG=/opt/cassandra/conf/cassandra.yaml

if [ "$CASSANDRA_LISTEN_ADDRESS" = "auto" ]; then
  export CASSANDRA_LISTEN_ADDRESS=$(hostname -i)
fi

: "${CASSANDRA_RPC_ADDRESS:=0.0.0.0}"

sed \
  -e "s/^cluster_name:.*/cluster_name: '${CASSANDRA_CLUSTER_NAME}'/" \
  -e "s/^listen_address:.*/listen_address: ${CASSANDRA_LISTEN_ADDRESS}/" \
  -e "s/^rpc_address:.*/rpc_address: ${CASSANDRA_RPC_ADDRESS}/" \
  -e "s/^seed_provider:.*/seed_provider:\n    - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n      parameters:\n        - seeds: \"${CASSANDRA_SEEDS}\"/" \
  /opt/cassandra/conf/cassandra.yaml.template > "$CONFIG"

export MAX_HEAP_SIZE=$CASSANDRA_HEAP_SIZE
export HEAP_NEWSIZE=$((CASSANDRA_HEAP_SIZE%M / 2))M

exec /opt/cassandra/bin/cassandra -f
