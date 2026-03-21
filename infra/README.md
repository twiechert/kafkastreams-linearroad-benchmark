# Linear Road Benchmark Infrastructure

Terraform stack to run the Linear Road benchmark on AWS using MSK Serverless + EC2.

## Architecture

```
┌─────────────────────┐
│  MSK Serverless     │  Kafka broker (auto-scaling, no management)
│  (eu-central-1)     │
└────────┬────────────┘
         │
    ┌────┴────┐
    │         │
┌───▼───┐ ┌──▼──────────────┐
│Feeder │ │ Workers (ASG)   │  N instances, each runs Kafka Streams
│  EC2  │ │ EC2 x N         │  with M threads. Partitions auto-distributed.
└───────┘ └─────────────────┘
```

**Components:**
- **MSK Serverless** — Kafka broker, IAM auth, no capacity planning
- **Feeder EC2** — generates data, pre-creates topics, pre-populates Kafka
- **Worker ASG** — Auto Scaling Group of Kafka Streams instances (same `application.id`, partitions auto-distributed)

## Partitioning Strategy

Topics are partitioned by `num_xways * 2` (one partition per expressway-direction pair):

| L (xways) | Partitions | Recommended Workers | Threads/Worker |
|-----------|------------|--------------------|--------------  |
| 1-5       | 12         | 2                  | 4              |
| 10        | 20         | 3                  | 4              |
| 25        | 50         | 5                  | 4              |
| 50        | 100        | 8                  | 4              |
| 100       | 200        | 15                 | 4              |

Most streams are keyed by `(xway, segment, direction)`. Since Kafka hashes the full key, records for the same expressway-direction will land on the same partition, enabling local joins.

## Usage

All infrastructure commands are available as `just` tasks from the project root. You can also run the Terraform commands directly.

### 1. Deploy

```bash
# Using justfile (from project root)
just infra-init
just infra-up my-key xways=10 workers=3

# Or directly with Terraform
cd infra/terraform
terraform init
terraform plan -var key_name=my-key -var num_xways=10 -var worker_count=3
terraform apply -var key_name=my-key -var num_xways=10 -var worker_count=3
```

### 2. Run Benchmark

```bash
# SSH to the feeder (using justfile or directly)
just ssh-feeder my-key
# or:
ssh -i ~/.ssh/my-key.pem ubuntu@$(terraform output -raw feeder_public_ip)

# Wait for setup to complete (check /var/log/feeder-init.log)
tail -f /var/log/feeder-init.log

# Run the benchmark
./run-benchmark.sh
```

### 3. Check Workers

```bash
# Workers auto-start via systemd. Check logs:
ssh -i ~/.ssh/my-key.pem ubuntu@<worker-ip>
journalctl -u lr-worker -f
```

### 4. Tear Down

```bash
# Using justfile
just infra-down my-key

# Or directly
terraform destroy -var key_name=my-key
```

## Scaling for Higher L-Ratings

To test higher L values:

```bash
# Scale up workers and xways
terraform apply \
  -var key_name=my-key \
  -var num_xways=50 \
  -var worker_count=8 \
  -var worker_instance_type=m5.2xlarge \
  -var vehicles_per_xway=500

# Re-run benchmark
ssh ubuntu@$(terraform output -raw feeder_public_ip)
./run-benchmark.sh
```

## Cost Estimate

| Component | Instance | Hourly Cost (eu-central-1) |
|-----------|----------|---------------------------|
| MSK Serverless | N/A | ~$0.10/GB ingested |
| Feeder | m5.large | ~$0.10/hr |
| Worker x3 | m5.xlarge | ~$0.20/hr each |
| **Total (3 workers)** | | **~$0.70/hr** |

Tear down immediately after benchmarking to avoid unnecessary costs.
