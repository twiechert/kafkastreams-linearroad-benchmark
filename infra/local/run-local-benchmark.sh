#!/bin/bash
set -euo pipefail

# ==============================================================================
# Local Linear Road Benchmark Runner
# Runs on macOS/Linux with Docker and Java 17+.
#
# Usage:
#   ./run-local-benchmark.sh                  # defaults: L=1, 3 min, 20 vehicles
#   ./run-local-benchmark.sh -x 5 -d 5 -v 50 # L=5, 5 min, 50 vehicles/xway
#   ./run-local-benchmark.sh -x 10 -t 8       # L=10, 8 stream threads
# ==============================================================================

# --- Defaults ---
NUM_XWAYS=1
DURATION_MINUTES=3
VEHICLES_PER_XWAY=20
STREAM_THREADS=4
PARTITIONS=12
BOOTSTRAP="localhost:9092"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- Parse args ---
while getopts "x:d:v:t:p:" opt; do
  case $opt in
    x) NUM_XWAYS=$OPTARG ;;
    d) DURATION_MINUTES=$OPTARG ;;
    v) VEHICLES_PER_XWAY=$OPTARG ;;
    t) STREAM_THREADS=$OPTARG ;;
    p) PARTITIONS=$OPTARG ;;
    *) echo "Usage: $0 [-x xways] [-d minutes] [-v vehicles] [-t threads] [-p partitions]"; exit 1 ;;
  esac
done

# Auto-calculate partitions if not explicitly set
if [ "$PARTITIONS" -lt $((NUM_XWAYS * 2)) ]; then
  PARTITIONS=$((NUM_XWAYS * 2))
fi

echo "============================================"
echo " Linear Road Local Benchmark"
echo "============================================"
echo " Expressways (L):     $NUM_XWAYS"
echo " Duration:            ${DURATION_MINUTES} min"
echo " Vehicles/xway:       $VEHICLES_PER_XWAY"
echo " Stream threads:      $STREAM_THREADS"
echo " Partitions:          $PARTITIONS"
echo "============================================"
echo ""

# --- Step 1: Start Kafka via Docker Compose ---
echo "[$(date +%T)] Starting Kafka..."
cd "$SCRIPT_DIR"

# Update partition count in docker-compose
export KAFKA_NUM_PARTITIONS=$PARTITIONS
docker compose up -d

echo "Waiting for Kafka to be healthy..."
for i in $(seq 1 30); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
    echo "Kafka is ready."
    break
  fi
  sleep 2
done

# --- Step 2: Pre-create topics with correct partition count ---
echo ""
echo "[$(date +%T)] Pre-creating topics ($PARTITIONS partitions)..."
for TOPIC in POS BALANCE DAILYEXP TOLL_HIST_TABLE; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true
  echo "  Created: $TOPIC"
done

# --- Step 3: Build the project ---
echo ""
echo "[$(date +%T)] Building project..."
cd "$PROJECT_DIR"
mvn clean package -Dmaven.test.skip=true -B -q

# --- Step 4: Generate test data ---
echo ""
echo "[$(date +%T)] Generating data..."
DATA_DIR="$PROJECT_DIR/benchmark-data"
mkdir -p "$DATA_DIR"

java -cp "target/kafka-linearroad-1.0-SNAPSHOT.jar:target/test-classes" \
  de.twiechert.linroad.kafka.benchmark.LinearRoadDataGenerator \
  -x "$NUM_XWAYS" -d "$DURATION_MINUTES" -v "$VEHICLES_PER_XWAY" \
  -o "$DATA_DIR/benchmark.dat"

echo "  Data: $(du -h "$DATA_DIR/benchmark.dat" | cut -f1)"
echo "  Tolls: $(du -h "$DATA_DIR/benchmark.dat.tolls.dat" | cut -f1)"

# --- Step 5: Run parallel Kafka Streams workers ---
echo ""
echo "[$(date +%T)] Starting $STREAM_THREADS-thread Kafka Streams worker..."

# Run the streaming application in the background
# It processes records from Kafka topics in parallel via stream threads
java -jar target/kafka-linearroad-1.0-SNAPSHOT.jar \
  --linearroad.data.path="$DATA_DIR/benchmark.dat" \
  --linearroad.hisotical.data.path="$DATA_DIR/benchmark.dat.tolls.dat" \
  --linearroad.kafka.bootstrapservers="$BOOTSTRAP" \
  --linearroad.mode=all \
  --linearroad.mode.debug=false \
  --linearroad.kafka.num_stream_threads="$STREAM_THREADS" \
  --spring.profiles.active=dev &

APP_PID=$!
echo "  Streams app PID: $APP_PID"

# --- Step 6: Wait and monitor ---
echo ""
echo "[$(date +%T)] Benchmark running. Press Ctrl+C to stop."
echo "  Monitor output in: $PROJECT_DIR/output/"

# Trap Ctrl+C to cleanup
cleanup() {
  echo ""
  echo "[$(date +%T)] Stopping..."
  kill $APP_PID 2>/dev/null || true
  wait $APP_PID 2>/dev/null || true

  echo ""
  echo "============================================"
  echo " Results"
  echo "============================================"
  if [ -d "$PROJECT_DIR/output" ]; then
    echo "Output files:"
    ls -lh "$PROJECT_DIR/output/" 2>/dev/null
    echo ""
    for f in "$PROJECT_DIR/output"/*.csv; do
      [ -f "$f" ] && echo "  $(basename "$f"): $(wc -l < "$f") records"
    done
  fi
  echo ""
  echo "To stop Kafka: cd $SCRIPT_DIR && docker compose down"
}

trap cleanup EXIT INT TERM
wait $APP_PID
