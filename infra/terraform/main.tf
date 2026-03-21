terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  # Partitions = xways * 2 (one per direction), minimum 12
  num_partitions = max(var.num_xways * 2, 12)

  # Input topics to pre-create with explicit partition counts
  input_topics = ["POS", "BALANCE", "DAILYEXP", "TOLL_HIST_TABLE"]

  common_tags = {
    Project = "linear-road-benchmark"
  }
}

# ============================================================
# Networking
# ============================================================

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(local.common_tags, { Name = "lr-benchmark-vpc" })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.common_tags, { Name = "lr-benchmark-igw" })
}

resource "aws_subnet" "az_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true
  tags                    = merge(local.common_tags, { Name = "lr-benchmark-a" })
}

resource "aws_subnet" "az_b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true
  tags                    = merge(local.common_tags, { Name = "lr-benchmark-b" })
}

resource "aws_route_table" "main" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = merge(local.common_tags, { Name = "lr-benchmark-rt" })
}

resource "aws_route_table_association" "a" {
  subnet_id      = aws_subnet.az_a.id
  route_table_id = aws_route_table.main.id
}

resource "aws_route_table_association" "b" {
  subnet_id      = aws_subnet.az_b.id
  route_table_id = aws_route_table.main.id
}

resource "aws_security_group" "main" {
  name_prefix = "lr-benchmark-"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  ingress {
    description = "Internal"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "lr-benchmark-sg" })
}

# ============================================================
# MSK Serverless
# ============================================================

resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "lr-benchmark"

  vpc_config {
    subnet_ids         = [aws_subnet.az_a.id, aws_subnet.az_b.id]
    security_group_ids = [aws_security_group.main.id]
  }

  client_authentication {
    sasl {
      iam { enabled = true }
    }
  }

  tags = local.common_tags
}

# ============================================================
# IAM for MSK access
# ============================================================

resource "aws_iam_role" "benchmark" {
  name = "lr-benchmark-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "msk_access" {
  name = "lr-msk-access"
  role = aws_iam_role.benchmark.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kafka-cluster:*"]
        Resource = ["${aws_msk_serverless_cluster.main.arn}/*", aws_msk_serverless_cluster.main.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["kafka:GetBootstrapBrokers", "kafka:DescribeClusterV2"]
        Resource = [aws_msk_serverless_cluster.main.arn]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "benchmark" {
  name = "lr-benchmark-profile"
  role = aws_iam_role.benchmark.name
}

# ============================================================
# Kafka Streams Workers (Auto Scaling Group)
# ============================================================

resource "aws_launch_template" "worker" {
  name_prefix   = "lr-worker-"
  image_id      = data.aws_ami.ubuntu.id
  instance_type = var.worker_instance_type
  key_name      = var.key_name

  iam_instance_profile {
    name = aws_iam_instance_profile.benchmark.name
  }

  vpc_security_group_ids = [aws_security_group.main.id]

  user_data = base64encode(templatefile("${path.module}/scripts/worker-init.sh", {
    cluster_arn        = aws_msk_serverless_cluster.main.arn
    aws_region         = var.aws_region
    num_stream_threads = var.streams_threads_per_worker
  }))

  tag_specifications {
    resource_type = "instance"
    tags          = merge(local.common_tags, { Name = "lr-streams-worker", Role = "worker" })
  }

  tags = local.common_tags
}

resource "aws_autoscaling_group" "workers" {
  name                = "lr-streams-workers"
  desired_capacity    = var.worker_count
  min_size            = 0
  max_size            = var.worker_count * 2
  vpc_zone_identifier = [aws_subnet.az_a.id, aws_subnet.az_b.id]

  launch_template {
    id      = aws_launch_template.worker.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "lr-streams-worker"
    propagate_at_launch = true
  }

  depends_on = [aws_msk_serverless_cluster.main]
}

# ============================================================
# Feeder / Orchestrator Instance
# ============================================================

resource "aws_instance" "feeder" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.feeder_instance_type
  key_name               = var.key_name
  subnet_id              = aws_subnet.az_a.id
  vpc_security_group_ids = [aws_security_group.main.id]
  iam_instance_profile   = aws_iam_instance_profile.benchmark.name

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/scripts/feeder-init.sh", {
    cluster_arn        = aws_msk_serverless_cluster.main.arn
    aws_region         = var.aws_region
    num_xways          = var.num_xways
    duration_minutes   = var.benchmark_duration_minutes
    vehicles_per_xway  = var.vehicles_per_xway
    num_partitions     = local.num_partitions
    input_topics       = join(" ", local.input_topics)
  })

  tags = merge(local.common_tags, { Name = "lr-benchmark-feeder", Role = "feeder" })

  depends_on = [aws_msk_serverless_cluster.main]
}
