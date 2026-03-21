#!/bin/bash
set -euo pipefail
exec > /var/log/worker-init.log 2>&1

echo "=== Linear Road Streams Worker Setup ==="

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

# --- Clone and build ---
WORK_DIR="/home/ubuntu/benchmark"
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"
git clone https://github.com/twiechert/kafka-streams-linearroad.git .
mvn clean package -Dmaven.test.skip=true -B

echo "Build complete."

# --- Write MSK IAM properties for Kafka Streams ---
cat > "$WORK_DIR/msk-iam.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
PROPS

# --- Download MSK IAM auth jar ---
MSK_IAM_VERSION="2.0.3"
wget -q -P "$WORK_DIR/" \
  "https://github.com/aws/aws-msk-iam-auth/releases/download/v$MSK_IAM_VERSION/aws-msk-iam-auth-$MSK_IAM_VERSION-all.jar"

# --- Create run script ---
# The worker runs the Kafka Streams application in "no-benchmark" mode
# (it only processes streams, does not feed data — the feeder does that)
cat > /home/ubuntu/run-worker.sh <<RUNEOF
#!/bin/bash
set -euo pipefail
cd /home/ubuntu/benchmark

echo "Starting Kafka Streams worker..."
echo "Bootstrap: $BOOTSTRAP"
echo "Threads: ${num_stream_threads}"

cat > /home/ubuntu/worker.properties <<PROPS
linearroad.data.path=/dev/null
linearroad.hisotical.data.path=/dev/null
linearroad.kafka.bootstrapservers=$BOOTSTRAP
linearroad.mode=no-benchmark
linearroad.mode.debug=false
linearroad.kafka.num_stream_threads=${num_stream_threads}
PROPS

java -cp "target/kafka-linearroad-1.0-SNAPSHOT.jar:aws-msk-iam-auth-2.0.3-all.jar" \
  de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication \
  /home/ubuntu/worker.properties
RUNEOF

chmod +x /home/ubuntu/run-worker.sh
chown ubuntu:ubuntu /home/ubuntu/run-worker.sh

# --- Create systemd service so workers auto-start ---
cat > /etc/systemd/system/lr-worker.service <<SVC
[Unit]
Description=Linear Road Kafka Streams Worker
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/benchmark
ExecStart=/home/ubuntu/run-worker.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable lr-worker
systemctl start lr-worker

echo "=== Worker started ==="
