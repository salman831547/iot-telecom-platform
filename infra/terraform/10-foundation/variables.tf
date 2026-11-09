variable "env" {
  description = "Environment name; selects config/envs/<env>.yaml (the single source of truth)."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.env))
    error_message = "env must be lowercase alphanumeric/hyphen, 2-16 chars."
  }
}

variable "config_dir" {
  description = "Directory holding the per-environment YAML config files."
  type        = string
  default     = null
}

variable "schema_dir" {
  description = "Directory holding BigQuery table definitions: <dataset_key>/<table>.json."
  type        = string
  default     = null
}

variable "pii_salt_version" {
  description = "Bump to rotate the PII hashing salt (write-only secret; never stored in state)."
  type        = number
  default     = 1
}
