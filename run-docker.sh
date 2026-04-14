#!/usr/bin/env bash
# Build image and run e2e-kafka with mounted config and output dir.
# Prerequisites: Docker, kafka-client.properties in current dir, GCP credentials for OAuth.

set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-kafka-e2e:latest}"

echo "Building Docker image: ${IMAGE}"
docker build -t "${IMAGE}" .

CONFIG="${CONFIG:-$(pwd)/kafka-client.properties}"
OUT_DIR="${OUT_DIR:-$(pwd)/out}"
mkdir -p "${OUT_DIR}"

if [[ ! -f "${CONFIG}" ]]; then
  echo "Missing ${CONFIG}. Copy kafka-client.properties.example and edit."
  exit 1
fi

# Optional: path to service account JSON (recommended in CI/containers)
SA_JSON="${GOOGLE_APPLICATION_CREDENTIALS:-}"

DOCKER_ENV=()
DOCKER_VOLS=(
  -v "${CONFIG}:/app/config/kafka-client.properties:ro"
  -v "${OUT_DIR}:/app/out"
)

if [[ -n "${SA_JSON}" && -f "${SA_JSON}" ]]; then
  DOCKER_VOLS+=(-v "${SA_JSON}:/app/secrets/gcp-sa.json:ro")
  DOCKER_ENV+=(-e GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-sa.json)
else
  # Local dev: mount gcloud ADC if present (macOS/Linux default path)
  ADC="${HOME}/.config/gcloud/application_default_credentials.json"
  if [[ -f "${ADC}" ]]; then
    DOCKER_VOLS+=(-v "${ADC}:/app/secrets/adc.json:ro")
    DOCKER_ENV+=(-e GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/adc.json)
  else
    echo "Warning: No GOOGLE_APPLICATION_CREDENTIALS and no gcloud ADC at ${ADC}"
    echo "Set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON path, or run: gcloud auth application-default login"
  fi
fi

TOPIC="${TOPIC:-YOUR_TOPIC}"
GROUP="${GROUP:-e2e-latency-docker}"
NUM="${NUM_MESSAGES:-50000}"

echo "Running (topic=${TOPIC}, messages=${NUM})..."
docker run --rm \
  "${DOCKER_VOLS[@]}" \
  "${DOCKER_ENV[@]}" \
  "${IMAGE}" \
  --kafkaProps /app/config/kafka-client.properties \
  --topic "${TOPIC}" \
  --groupId "${GROUP}" \
  --numMessages "${NUM}" \
  --sampleEverySeconds 1 \
  --outputDir /app/out

echo "Reports under: ${OUT_DIR}"
