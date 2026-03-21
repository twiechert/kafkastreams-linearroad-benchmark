output "msk_cluster_arn" {
  description = "ARN of the MSK Serverless cluster"
  value       = aws_msk_serverless_cluster.main.arn
}

output "feeder_public_ip" {
  description = "Public IP of the feeder/orchestrator instance"
  value       = aws_instance.feeder.public_ip
}

output "num_partitions" {
  description = "Number of partitions per topic (= max(num_xways * 2, 12))"
  value       = local.num_partitions
}

output "worker_count" {
  description = "Number of Kafka Streams worker instances"
  value       = var.worker_count
}

output "ssh_to_feeder" {
  description = "SSH command to connect to the feeder"
  value       = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.feeder.public_ip}"
}

output "run_benchmark" {
  description = "Command to run the benchmark after SSH"
  value       = "./run-benchmark.sh"
}
