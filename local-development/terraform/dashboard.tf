resource "grafana_dashboard" "flink_cdc_poc" {
  config_json = file("${path.module}/../grafana/provisioning/dashboards/flink-cdc-monitoring.json")

  depends_on = [grafana_data_source.prometheus]
}
