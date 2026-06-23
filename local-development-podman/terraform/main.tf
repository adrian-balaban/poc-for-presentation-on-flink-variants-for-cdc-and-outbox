terraform {
  required_version = ">= 1.6"

  required_providers {
    grafana = {
      source  = "grafana/grafana"
      version = "~> 3.4"
    }
  }

  # Local state — fine for a dev POC; swap for S3 backend in production
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "grafana" {
  url  = var.grafana_url
  auth = var.grafana_auth
}
