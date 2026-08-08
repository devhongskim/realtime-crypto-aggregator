output "bucket_arn" {
  description = "ARN of the crypto streaming pipeline's S3 bucket"
  value       = aws_s3_bucket.hongskim-crypto-stream.arn
}

output "bucket_name" {
  description = "Name of the crypto streaming pipeline's S3 bucket"
  value       = aws_s3_bucket.hongskim-crypto-stream.bucket
}
