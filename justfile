# Linear Road Benchmark - Task Runner
# Install just: https://github.com/casey/just#installation

set dotenv-load := false

# Default recipe
default:
  @just --list

# ============================================================
# Build
# ============================================================

# Build the project (skip tests)
build:
  mvn clean package -Dmaven.test.skip=true -B

# Compile only
compile:
  mvn clean compile -B

# Run unit tests (excludes benchmark tests)
test:
  mvn test -B -Dtest='!*Benchmark*' -DfailIfNoTests=false

# Run all tests including benchmark
test-all:
  mvn test -B

# ============================================================
# Local Benchmark (Docker + local JVM)
# ============================================================

# Start local Kafka (single-node KRaft via Docker)
kafka-up partitions="12":
  cd infra/local && KAFKA_NUM_PARTITIONS={{partitions}} docker compose up -d
  @echo "Waiting for Kafka..."
  @for i in $(seq 1 30); do \
    docker compose -f infra/local/docker-compose.yml exec -T kafka \
      kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null && break; \
    sleep 2; \
  done
  @echo "Kafka is ready on localhost:9092"

# Stop local Kafka
kafka-down:
  cd infra/local && docker compose down -v

# Pre-create topics with correct partition count
topics-create partitions="12": (kafka-up partitions)
  #!/usr/bin/env bash
  for TOPIC in POS BALANCE DAILYEXP TOLL_HIST_TABLE; do
    docker compose -f infra/local/docker-compose.yml exec -T kafka \
      kafka-topics --bootstrap-server localhost:9092 \
      --create --topic "$TOPIC" --partitions {{partitions}} \
      --replication-factor 1 --if-not-exists 2>/dev/null
    echo "  Created: $TOPIC ({{partitions}} partitions)"
  done

# List all Kafka topics
topics-list:
  docker compose -f infra/local/docker-compose.yml exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 --list --exclude-internal

# Generate test data
generate xways="1" duration="3" vehicles="20":
  #!/usr/bin/env bash
  mkdir -p benchmark-data
  java -cp "target/kafka-linearroad-1.0-SNAPSHOT.jar:target/test-classes" \
    de.twiechert.linroad.kafka.benchmark.LinearRoadDataGenerator \
    -x {{xways}} -d {{duration}} -v {{vehicles}} -o benchmark-data/benchmark.dat
  echo "Generated:"
  ls -lh benchmark-data/

# Run the full local benchmark end-to-end
local xways="1" duration="3" vehicles="20" threads="4": build
  #!/usr/bin/env bash
  set -euo pipefail
  PARTITIONS=$(( {{xways}} * 2 > 12 ? {{xways}} * 2 : 12 ))

  echo "============================================"
  echo " Linear Road Local Benchmark"
  echo " L={{xways}}  duration={{duration}}min  vehicles={{vehicles}}/xway"
  echo " threads={{threads}}  partitions=$PARTITIONS"
  echo "============================================"

  # Start Kafka + create topics
  just kafka-up $PARTITIONS
  just topics-create $PARTITIONS

  # Generate data
  just generate {{xways}} {{duration}} {{vehicles}}

  # Run the streaming application
  mkdir -p output
  echo ""
  echo "Starting Kafka Streams ({{threads}} threads)..."
  java -jar target/kafka-linearroad-1.0-SNAPSHOT.jar \
    --linearroad.data.path=benchmark-data/benchmark.dat \
    --linearroad.hisotical.data.path=benchmark-data/benchmark.dat.tolls.dat \
    --linearroad.kafka.bootstrapservers=localhost:9092 \
    --linearroad.mode=all \
    --linearroad.mode.debug=false \
    --linearroad.kafka.num_stream_threads={{threads}} \
    --spring.profiles.active=dev

# Run the convenience local script (alternative)
local-script *ARGS:
  cd infra/local && ./run-local-benchmark.sh {{ARGS}}

# ============================================================
# AWS Infrastructure (Terraform)
# ============================================================

# Initialize Terraform
infra-init:
  cd infra/terraform && terraform init

# Plan AWS infrastructure
infra-plan key_name xways="10" workers="3":
  cd infra/terraform && terraform plan \
    -var key_name={{key_name}} \
    -var num_xways={{xways}} \
    -var worker_count={{workers}}

# Deploy AWS infrastructure (MSK Serverless + EC2 workers + feeder)
infra-up key_name xways="10" workers="3":
  cd infra/terraform && terraform apply -auto-approve \
    -var key_name={{key_name}} \
    -var num_xways={{xways}} \
    -var worker_count={{workers}}

# Destroy AWS infrastructure
infra-down key_name:
  cd infra/terraform && terraform destroy -auto-approve \
    -var key_name={{key_name}}

# SSH to the feeder instance
ssh-feeder key_name:
  ssh -i ~/.ssh/{{key_name}}.pem ubuntu@$(cd infra/terraform && terraform output -raw feeder_public_ip)

# Show Terraform outputs
infra-status:
  cd infra/terraform && terraform output

# ============================================================
# Utilities
# ============================================================

# Clean build artifacts and generated data
clean:
  mvn clean -q
  rm -rf benchmark-data output

# Full clean including Docker volumes
clean-all: clean kafka-down
