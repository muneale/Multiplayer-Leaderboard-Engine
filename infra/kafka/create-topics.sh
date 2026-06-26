#!/usr/bin/env sh
set -eu

KAFKA_ADDR="${KAFKA_ADDR:-localhost:9092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

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
