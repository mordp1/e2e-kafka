# e2e-kafka

Java (containerized build, default Java 21 runtime) end-to-end Kafka latency test app for GCP Managed Kafka with OAuth (OAUTHBEARER), using Kafka clients 4.x.

## Versioned releases

| Tag | Kafka client | Notes |
|-----|-------------|-------|
| [`v1.0.0-kafka-3.7-gcp`](https://github.com/mordp1/e2e-kafka/tree/v1.0.0-kafka-3.7-gcp) | 3.7.2 | Kafka 3.7.x + GCP Managed Kafka OAuth — use this if you need 3.7.x compatibility |
| `main` | 4.x (latest) | Tracks the latest Kafka client release |

To use the Kafka 3.7.x version:

```bash
git checkout v1.0.0-kafka-3.7-gcp
```

---

## What it does

- Produces invoice-like JSON messages to a Kafka topic
- Consumes the same messages with a consumer group
- Measures producer-to-consumer latency using message header `sendTsMs`
- Generates:
  - JSON report (`out/report-<runId>.json`)
  - HTML Plotly report (`out/report-<runId>.html`)

Reference:

- [GCP Managed Kafka Java quickstart](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/quickstart-java)

---

## Option A — Build and run with Docker (recommended)

You do **not** need local Java or Maven installed on your machine. Only **Docker** is required.

If you see this local error:

`error: release version 25 not supported`

skip local Maven and use Docker build/run below.

### 1. Prepare config

```bash
cd e2e-kafka
cp kafka-client.properties.example kafka-client.properties
# Edit kafka-client.properties — set bootstrap.servers and keep OAuth settings
```

### 2. GCP credentials inside the container

The app uses **Application Default Credentials** (same as the GCP quickstart). Pick one:

**A) Service account JSON (CI, servers, explicit file)**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/sa.json
./run-docker.sh
```

`run-docker.sh` mounts that file into the container and sets `GOOGLE_APPLICATION_CREDENTIALS` there.

**B) gcloud ADC (local laptop)**

```bash
gcloud auth application-default login
./run-docker.sh
```

If `~/.config/gcloud/application_default_credentials.json` exists, the script mounts it automatically.

### 3. Build image and run

```bash
chmod +x run-docker.sh
export TOPIC=your-topic-name
export NUM_MESSAGES=50000
./run-docker.sh
```

Or manually:

```bash
docker build -t kafka-e2e:latest .

mkdir -p out
docker run --rm \
  -v "$(pwd)/kafka-client.properties:/app/config/kafka-client.properties:ro" \
  -v "$(pwd)/out:/app/out" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/sa.json \
  -v "/absolute/path/to/sa.json:/app/secrets/sa.json:ro" \
  kafka-e2e:latest \
  --kafkaProps /app/config/kafka-client.properties \
  --topic YOUR_TOPIC \
  --groupId e2e-latency \
  --numMessages 50000 \
  --sampleEverySeconds 1 \
  --outputDir /app/out
```

Reports appear in `./out/` on the host.

### Extract the JAR from the image (run anywhere with only JRE 25)

After `docker build`, you can copy the fat JAR out without running the test:

```bash
docker create --name kafka-e2e-extract kafka-e2e:latest
docker cp kafka-e2e-extract:/app/e2e-kafka.jar ./e2e-kafka.jar
docker rm kafka-e2e-extract
```

Copy `e2e-kafka.jar` and `kafka-client.properties` to any host with JRE 21+ and run:

```bash
java -jar e2e-kafka.jar --kafkaProps kafka-client.properties --topic YOUR_TOPIC --outputDir out
```

```bash
java -jar e2e-kafka.jar --kafkaProps /tmp/client.properties --topic test-e2e --groupId e2e-latency --mode continuous --ratePerSecond 2000 --durationSeconds 360 --messageType invoice --keyStrategy none --sampleEverySeconds 1 --outputDir /tmp
```

Or use the helper script:

```bash
chmod +x build-jar-docker.sh
./build-jar-docker.sh
```

Generated file:

```text
dist/e2e-kafka.jar
```

### Docker build args (override defaults if needed)

Default images are Temurin 21 and should work. You can still override base images:

```bash
docker build \
  --build-arg MAVEN_IMAGE=maven:3.9.9-eclipse-temurin-21 \
  --build-arg JRE_IMAGE=eclipse-temurin:21-jre-jammy \
  -t kafka-e2e:latest .
```

If you change Java version in the image, align `maven.compiler.release` in `pom.xml` with that JDK.

---

## Option B — Local build (fat JAR you can copy anywhere)

Requires **JDK 21+** and **Maven 3.9+** on the machine where you build.

### Build the runnable JAR

```bash
cd e2e-kafka
mvn -q clean package -DskipTests
```

Output artifact (includes all dependencies):

```text
target/e2e-kafka.jar
```

Copy **only** `e2e-kafka.jar` plus `kafka-client.properties` to any host that has **JRE 21+** (or compatible runtime).

### Run the JAR on another machine

```bash
java -jar e2e-kafka.jar \
  --kafkaProps kafka-client.properties \
  --topic YOUR_TOPIC \
  --groupId e2e-latency \
  --numMessages 50000 \
  --sampleEverySeconds 1 \
  --outputDir out
```

Set GCP credentials on that host:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
# or use gcloud ADC on that machine
```

---

## Option C — Local run with Maven (development)

```bash
cp kafka-client.properties.example kafka-client.properties
# edit file

mvn -q clean package
mvn -q exec:java -Dexec.args="--kafkaProps kafka-client.properties --topic YOUR_TOPIC --groupId e2e-latency --numMessages 50000 --sampleEverySeconds 1 --outputDir out"
```

---

## CLI arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--kafkaProps` | `kafka-client.properties` | Path to client properties |
| `--topic` | `invoice-topic` | Topic name |
| `--groupId` | `invoice-e2e-group` | Consumer group base (runId appended automatically) |
| `--messageType` | `invoice` | Message schema: `invoice` \| `payment` \| `order` |
| `--mode` | `batch` | Run mode: `batch` (send N total) \| `continuous` (rate-limited) |
| `--numMessages` | `10000` | Total messages — **batch mode only** |
| `--ratePerSecond` | `500` | Target send rate — **continuous mode only** (msg/s) |
| `--durationSeconds` | `60` | Run duration — **continuous mode only** (seconds) |
| `--keyStrategy` | `messageId` | Kafka record key: `messageId` \| `entityId` \| `none` |
| `--sampleEverySeconds` | `1` | Chart sampling interval |
| `--maxWaitSeconds` | `120` | Max wait for consumer after producer finishes |
| `--outputDir` | `out` | Reports directory |

---

## Output

- `out/report-<runId>.json`
- `out/report-<runId>.html` — open in a browser for Plotly charts

---

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| `docker build` fails with image tag not found | Use the default tags in Dockerfile (Temurin 21) or set `--build-arg` to known existing tags |
| OAuth / authentication errors | Confirm `GOOGLE_APPLICATION_CREDENTIALS` or gcloud ADC inside container |
| Consumer never finishes | Wrong topic, ACLs, or new consumer group on empty topic; check `auto.offset.reset` and that producer ran |
| Permission denied on `out/` | Ensure mounted directory is writable by container user (host folder permissions) |

### Verify messages are really produced

The app now counts a message as produced **only after Kafka ack** (`send(...).get(...)`).
At the end it prints:

- `Produced OK`
- `failed sends`
- `consumed`

If `failed sends > 0` or `consumed < produced`, check topic ACLs/auth and consumer group settings.
