### Step 1: Prepare Rakam Collector ###
FROM maven:3-jdk-8 as rakam-presto-collector
WORKDIR /build
# Copy source and create package AND Install maven packages
ADD . .
RUN mvn clean install -T 1C -DskipTests=true

### Step 2: Bundle builds ###
FROM openjdk:8-jre-alpine
# Copy source from target
COPY --from=rakam-presto-collector /build/target /compiled/target
# Set entrypoint
# ENTRYPOINT /compiled/target/rakam-*-collector/rakam-*/bin/launcher run
EXPOSE 9997