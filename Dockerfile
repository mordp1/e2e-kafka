# syntax=docker/dockerfile:1
#
# Build the fat JAR inside Docker — no local Java/Maven required.
# Defaults use broadly available Temurin 21 tags.
#
ARG MAVEN_IMAGE=maven:3.9.9-eclipse-temurin-21
ARG JRE_IMAGE=eclipse-temurin:21-jre-jammy

FROM ${MAVEN_IMAGE} AS builder
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM ${JRE_IMAGE}
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app app
COPY --from=builder /build/target/e2e-kafka.jar /app/e2e-kafka.jar
USER app
ENTRYPOINT ["java", "-jar", "/app/e2e-kafka.jar"]
