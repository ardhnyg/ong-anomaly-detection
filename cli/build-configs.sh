#!/usr/bin/env bash
# Membuat file config connector Datagen (Confluent Cloud) dari file .avsc.
# Pakai: KAFKA_API_KEY=xxx KAFKA_API_SECRET=yyy ./build-configs.sh
# Butuh: jq. Hasil: connector-<nama>.json di folder ini (berisi secret, jangan di-commit).
set -euo pipefail
cd "$(dirname "$0")/../datagen"

: "${KAFKA_API_KEY:?Set KAFKA_API_KEY}"
: "${KAFKA_API_SECRET:?Set KAFKA_API_SECRET}"
TOPIC="${TOPIC:-pump_telemetry}"

build () {
  local name="$1" interval="$2"
  jq -n \
    --arg name "pdm-datagen-${name}" \
    --arg key "$KAFKA_API_KEY" \
    --arg secret "$KAFKA_API_SECRET" \
    --arg topic "$TOPIC" \
    --arg interval "$interval" \
    --rawfile schema "${name}.avsc" \
    '{
      "name": $name,
      "connector.class": "DatagenSource",
      "kafka.auth.mode": "KAFKA_API_KEY",
      "kafka.api.key": $key,
      "kafka.api.secret": $secret,
      "kafka.topic": $topic,
      "output.data.format": "AVRO",
      "schema.string": ($schema | fromjson | tojson),
      "schema.keyfield": "asset_id",
      "max.interval": $interval,
      "tasks.max": "1"
    }' > "connector-${name}.json"
  echo "connector-${name}.json"
}

# Normal: 6 aset, total sekitar 4 pesan/detik.
build pump-normal 500
# Degradasi dan gagal: 1 aset, sekitar 5 pesan/detik, jadi mendominasi rata-rata per window.
build pump-degradasi 400
build pump-gagal 400
