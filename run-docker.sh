#!/usr/bin/env bash
# Build image and run e2e-kafka with mounted config, secrets, and output dir.
#
# Prerequisites: Docker, a kafka-client.properties in current dir (or set CONFIG=).
#
# Environment variables:
#   CONFIG                        Path to kafka-client.properties (default: ./kafka-client.properties)
#   OUT_DIR                       Output dir for reports          (default: ./out)
#   IMAGE                         Docker image tag                (default: kafka-e2e:latest)
#   TOPIC                         Kafka topic                     (default: YOUR_TOPIC)
#   GROUP                         Consumer group base             (default: e2e-latency-docker)
#   NUM_MESSAGES                  Messages to send (batch mode)   (default: 50000)
#
# TLS / mTLS secrets (optional — mount into /app/secrets/ inside container):
#   SSL_TRUSTSTORE                Path to client.truststore.jks on the host
#   SSL_KEYSTORE                  Path to client.keystore.jks on the host  (mTLS only)
#
# GCP OAuth credentials (optional — pick one):
#   GOOGLE_APPLICATION_CREDENTIALS  Path to service-account JSON on the host
#   (if absent, gcloud ADC at ~/.config/gcloud/application_default_credentials.json is used)

set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-kafka-e2e:latest}"

echo "Building Docker image: ${IMAGE}"
docker build -t "${IMAGE}" .

CONFIG="${CONFIG:-$(pwd)/kafka-client.properties}"
OUT_DIR="${OUT_DIR:-$(pwd)/out}"
mkdir -p "${OUT_DIR}"

if [[ ! -f "${CONFIG}" ]]; then
  echo "ERROR: Config not found at ${CONFIG}"
  echo "  Copy one of the examples/ files, fill in your values, and point CONFIG= at it."
  echo "  Example:  cp examples/scram-sha-256.properties kafka-client.properties"
  exit 1
fi

DOCKER_ENV=()
DOCKER_VOLS=(
  -v "${CONFIG}:/app/config/kafka-client.properties:ro"
  -v "${OUT_DIR}:/app/out"
)

# ── TLS truststore ────────────────────────────────────────────────────────────
SSL_TRUSTSTORE="${SSL_TRUSTSTORE:-}"
if [[ -n "${SSL_TRUSTSTORE}" && -f "${SSL_TRUSTSTORE}" ]]; then
  DOCKER_VOLS+=(-v "${SSL_TRUSTSTORE}:/app/secrets/client.truststore.jks:ro")
fi

# ── mTLS keystore ─────────────────────────────────────────────────────────────
SSL_KEYSTORE="${SSL_KEYSTORE:-}"
if [[ -n "${SSL_KEYSTORE}" && -f "${SSL_KEYSTORE}" ]]; then
  DOCKER_VOLS+=(-v "${SSL_KEYSTORE}:/app/secrets/client.keystore.jks:ro")
fi

# ── GCP OAuth credentials ─────────────────────────────────────────────────────
SA_JSON="${GOOGLE_APPLICATION_CREDENTIALS:-}"
if [[ -n "${SA_JSON}" && -f "${SA_JSON}" ]]; then
  DOCKER_VOLS+=(-v "${SA_JSON}:/app/secrets/gcp-sa.json:ro")
  DOCKER_ENV+=(-e GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-sa.json)
else
  ADC="${HOME}/.config/gcloud/application_default_credentials.json"
  if [[ -f "${ADC}" ]]; then
    DOCKER_VOLS+=(-v "${ADC}:/app/secrets/adc.json:ro")
    DOCKER_ENV+=(-e GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/adc.json)
  fi
fi

TOPIC="${TOPIC:-YOUR_TOPIC}"
GROUP="${GROUP:-e2e-latency-docker}"
NUM="${NUM_MESSAGES:-50000}"

echo "Running (topic=${TOPIC}, messages=${NUM})..."
docker run --rm \
  "${DOCKER_VOLS[@]}" \
  "${DOCKER_ENV[@]+"${DOCKER_ENV[@]}"}" \
  "${IMAGE}" \
  --kafkaProps /app/config/kafka-client.properties \
  --topic "${TOPIC}" \
  --groupId "${GROUP}" \
  --numMessages "${NUM}" \
  --sampleEverySeconds 1 \
  --outputDir /app/out

echo "Reports under: ${OUT_DIR}"
