# Greenfield resource: unlike the batch project's bucket (which was imported
# from pre-existing infrastructure), this bucket does not exist yet -- Terraform
# creates it directly via `terraform apply`, no `terraform import` step needed.

resource "aws_s3_bucket" "hongskim-crypto-stream" {
  bucket = var.bucket_name

  tags = {
    Environment = "Production"
    Project     = "hongskim-crypto-stream"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.hongskim-crypto-stream.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
