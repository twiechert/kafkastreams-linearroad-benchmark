variable "aws_region" {
  description = "AWS region to deploy the cluster"
  type        = string
  default     = "eu-central-1"
}

variable "worker_instance_type" {
  description = "EC2 instance type for Kafka Streams worker nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "worker_count" {
  description = "Number of Kafka Streams worker instances. Each gets a share of partitions."
  type        = number
  default     = 3
}

variable "feeder_instance_type" {
  description = "EC2 instance type for the data feeder/orchestrator"
  type        = string
  default     = "m5.large"
}

variable "key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
}

variable "num_xways" {
  description = "Number of expressways (L-rating target). Determines partition count = num_xways * 2."
  type        = number
  default     = 10
}

variable "benchmark_duration_minutes" {
  description = "Simulated benchmark duration in minutes"
  type        = number
  default     = 30
}

variable "vehicles_per_xway" {
  description = "Number of vehicles per expressway"
  type        = number
  default     = 200
}

variable "streams_threads_per_worker" {
  description = "Number of Kafka Streams threads per worker instance"
  type        = number
  default     = 4
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH into instances"
  type        = string
  default     = "0.0.0.0/0"
}
