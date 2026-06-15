resource "grafana_data_source" "prometheus" {
  type       = "prometheus"
  name       = "Prometheus"
  uid        = "prometheus"
  url        = "http://prometheus:9090"
  is_default = true
}
