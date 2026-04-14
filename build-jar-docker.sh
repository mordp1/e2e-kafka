#!/usr/bin/env bash
# Build fat JAR entirely inside Docker (no local Java/Maven required)
# Output: ./dist/e2e-kafka.jar

set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-kafka-e2e:build}"
DIST_DIR="${DIST_DIR:-$(pwd)/dist}"
mkdir -p "${DIST_DIR}"

echo "Building Docker image: ${IMAGE}"
docker build -t "${IMAGE}" .

CID=$(docker create "${IMAGE}")
trap 'docker rm -f "${CID}" >/dev/null 2>&1 || true' EXIT

docker cp "${CID}:/app/e2e-kafka.jar" "${DIST_DIR}/e2e-kafka.jar"

echo "Done."
echo "JAR generated at: ${DIST_DIR}/e2e-kafka.jar"
echo
echo "Run anywhere with JRE 21+:"
echo "  java -jar ${DIST_DIR}/e2e-kafka.jar --kafkaProps kafka-client.properties --topic YOUR_TOPIC --outputDir out"
