# Build the connector JAR
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline

COPY src/ src/
RUN mvn clean package -DskipTests

# Use Confluent's Kafka Connect base image
FROM confluentinc/cp-kafka-connect:7.5.3

# Copy the connector JAR with dependencies
COPY --from=builder /build/target/kafka-connect-xtdb-1.0.0-SNAPSHOT-jar-with-dependencies.jar /usr/share/java/kafka-connect-xtdb/

# The connector will be available at startup
