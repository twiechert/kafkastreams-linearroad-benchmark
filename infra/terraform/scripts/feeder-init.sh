#!/bin/bash
set -euo pipefail
exec > /var/log/feeder-init.log 2>&1

echo "=== Linear Road Feeder / Orchestrator Setup ==="

# --- Install dependencies ---
apt-get update -qq
apt-get install -y -qq openjdk-17-jdk-headless maven git awscli

# --- Discover MSK bootstrap servers ---
echo "Waiting for MSK cluster..."
for i in $(seq 1 30); do
  STATE=$(aws kafka describe-cluster-v2 \
    --cluster-arn "${cluster_arn}" \
    --region "${aws_region}" \
    --query 'ClusterInfo.State' --output text 2>/dev/null || echo "UNKNOWN")
  [ "$STATE" = "ACTIVE" ] && break
  echo "  Attempt $i/30: $STATE"
  sleep 30
done

BOOTSTRAP=$(aws kafka get-bootstrap-brokers \
  --cluster-arn "${cluster_arn}" \
  --region "${aws_region}" \
  --query 'BootstrapBrokerStringSaslIam' --output text)
echo "Bootstrap: $BOOTSTRAP"

# --- Download Kafka CLI for topic management ---
KAFKA_VERSION="3.7.0"
KAFKA_DIR="/opt/kafka"
mkdir -p "$KAFKA_DIR"
cd /tmp
wget -q "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"
tar -xzf "kafka_2.13-$KAFKA_VERSION.tgz" -C "$KAFKA_DIR" --strip-components=1
rm -f "kafka_2.13-$KAFKA_VERSION.tgz"

# --- MSK IAM auth ---
MSK_IAM_VERSION="2.0.3"
wget -q -P "$KAFKA_DIR/libs/" \
  "https://github.com/aws/aws-msk-iam-auth/releases/download/v$MSK_IAM_VERSION/aws-msk-iam-auth-$MSK_IAM_VERSION-all.jar"

cat > /tmp/client.properties <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
PROPS

# --- Pre-create input topics ---
echo "=== Pre-creating topics (${num_partitions} partitions each) ==="
for TOPIC in ${input_topics}; do
  echo "  Creating: $TOPIC"
  $KAFKA_DIR/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config /tmp/client.properties \
    --create --topic "$TOPIC" \
    --partitions ${num_partitions} \
    --if-not-exists 2>&1 || true
done
echo "Topics created."

# --- Clone and build ---
WORK_DIR="/home/ubuntu/benchmark"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"
git clone https://github.com/twiechert/kafka-streams-linearroad.git .
mvn clean package -Dmaven.test.skip=true -B

# --- Download MSK IAM jar for the app ---
wget -q -P "$WORK_DIR/" \
  "https://github.com/aws/aws-msk-iam-auth/releases/download/v$MSK_IAM_VERSION/aws-msk-iam-auth-$MSK_IAM_VERSION-all.jar"

# --- Generate test data ---
echo "=== Generating data: L=${num_xways}, ${duration_minutes}min, ${vehicles_per_xway} vehicles/xway ==="
DATA_DIR="$WORK_DIR/benchmark-data"
mkdir -p "$DATA_DIR"

java -cp "target/kafka-linearroad-1.0-SNAPSHOT.jar:target/test-classes" \
  de.twiechert.linroad.kafka.benchmark.LinearRoadDataGenerator \
  -x ${num_xways} -d ${duration_minutes} -v ${vehicles_per_xway} \
  -o "$DATA_DIR/benchmark.dat"

echo "Data generated."
ls -lh "$DATA_DIR/"

# --- Write the run script ---
# This script:
#   1. Pre-populates Kafka topics with all generated data (fast batch write)
#   2. Signals workers to start processing (they are already running via systemd)
#   3. Waits for processing to complete and checks output latencies
cat > /home/ubuntu/run-benchmark.sh <<'RUNEOF'
#!/bin/bash
set -euo pipefail

WORK_DIR="/home/ubuntu/benchmark"
DATA_DIR="$WORK_DIR/benchmark-data"
BOOTSTRAP="__BOOTSTRAP__"
NUM_XWAYS=__NUM_XWAYS__

echo "============================================"
echo " Linear Road Benchmark"
echo " L = $NUM_XWAYS expressways"
echo " Bootstrap: $BOOTSTRAP"
echo "============================================"

cd "$WORK_DIR"
mkdir -p output

# Phase 1: Pre-populate Kafka topics with ALL data at once.
# The Kafka Streams workers (running on other EC2 instances) will
# consume and process these records. Pre-populating is faster than
# real-time feeding and lets us measure pure processing throughput.
echo ""
echo "[$(date +%T)] Phase 1: Pre-populating Kafka topics..."
FEED_START=$(date +%s%3N)

cat > "$DATA_DIR/cluster.properties" <<PROPS
linearroad.data.path=$DATA_DIR/benchmark.dat
linearroad.hisotical.data.path=$DATA_DIR/benchmark.dat.tolls.dat
linearroad.kafka.bootstrapservers=$BOOTSTRAP
linearroad.mode=all
linearroad.mode.debug=false
linearroad.kafka.num_stream_threads=0
PROPS

java -cp "target/kafka-linearroad-1.0-SNAPSHOT.jar:aws-msk-iam-auth-2.0.3-all.jar" \
  de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication \
  "$DATA_DIR/cluster.properties"

FEED_END=$(date +%s%3N)
FEED_DURATION_MS=$((FEED_END - FEED_START))
echo "[$(date +%T)] Pre-population complete in ${FEED_DURATION_MS}ms"

# Phase 2: Monitor output topics for results.
# The workers are processing records. Check for output.
echo ""
echo "[$(date +%T)] Phase 2: Monitoring output..."
echo "Workers are processing. Check worker logs with:"
echo "  ssh ubuntu@<worker-ip> journalctl -u lr-worker -f"
echo ""
echo "Output topics to check:"
echo "  TOLL_NOT (toll notifications)"
echo "  ACC_NOT (accident notifications)"
echo "  ACCOUNT_BALANCE_RESP (account balance responses)"
echo "  DAILY_EXP_RESP (daily expenditure responses)"
echo ""

# Count output records
sleep 30  # give workers time to process
for TOPIC in TOLL_NOT ACC_NOT ACCOUNT_BALANCE_RESP DAILY_EXP_RESP; do
  COUNT=$(/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$BOOTSTRAP" \
    --command-config /tmp/client.properties \
    --topic "$TOPIC" 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
  echo "  $TOPIC: $COUNT records"
done

echo ""
echo "============================================"
echo " Benchmark Complete"
echo " Feed time: ${FEED_DURATION_MS}ms"
echo "============================================"
RUNEOF

# Replace placeholders
sed -i "s|__BOOTSTRAP__|$BOOTSTRAP|g" /home/ubuntu/run-benchmark.sh
sed -i "s|__NUM_XWAYS__|${num_xways}|g" /home/ubuntu/run-benchmark.sh
chmod +x /home/ubuntu/run-benchmark.sh
chown -R ubuntu:ubuntu /home/ubuntu/

echo ""
echo "=== Feeder Setup Complete ==="
echo "SSH in and run: ./run-benchmark.sh"
