variable "grafana_url" {
  description = "Grafana base URL"
  type        = string
  default     = "http://localhost:3001"
}

variable "grafana_auth" {
  description = "Grafana credentials in user:password format"
  type        = string
  default     = "admin:admin"
  sensitive   = true
}
