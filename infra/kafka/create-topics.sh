#!/usr/bin/env sh
set -eu

KAFKA_ADDR="${KAFKA_ADDR:-localhost:9092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
export PATH="$PATH:/opt/kafka/bin"

# Wait until Kafka is responsive to topic queries
echo "Waiting for Kafka at $KAFKA_ADDR..."
until kafka-topics.sh --bootstrap-server "$KAFKA_ADDR" --list >/dev/null 2>&1; do
  sleep 1
done
echo "Kafka is ready."

create_topic() {
  topic="$1"
  partitions="${2:-3}"

  if kafka-topics.sh --bootstrap-server "$KAFKA_ADDR" --list 2>/dev/null | grep -qxF "$topic"; then
    echo "topic already exists: $topic"
    return
  fi

  kafka-topics.sh \
    --bootstrap-server "$KAFKA_ADDR" \
    --create \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config retention.ms=604800000

  echo "created topic: $topic (partitions=$partitions)"
}

create_topic "score-events" 3
create_topic "player-events" 3
