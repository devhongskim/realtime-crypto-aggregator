variable "bucket_name" {
  description = "Name of the S3 bucket dedicated to this streaming project's Bronze/Gold trade data. Must be globally unique across all of AWS."
  type        = string
}
