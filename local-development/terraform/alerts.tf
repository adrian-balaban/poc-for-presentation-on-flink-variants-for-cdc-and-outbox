# Alert rules (mirroring the Datadog monitors).
# Folder and contact point are created here so the stack is self-contained.

resource "grafana_folder" "flink_cdc_poc" {
  title = "Flink CDC POC"
}

resource "grafana_contact_point" "default_email" {
  name = "flink-cdc-poc-email"
  email {
    addresses               = ["adrian.n.balaban@gmail.com"]
    single_email            = false
    disable_resolve_message = false
  }
}

resource "grafana_notification_policy" "flink_cdc_poc" {
  contact_point = grafana_contact_point.default_email.name
  group_by      = ["alertname", "job_name"]

  depends_on = [grafana_contact_point.default_email]
}

resource "grafana_rule_group" "flink_cdc_poc" {
  name             = "flink-cdc-poc-alerts"
  folder_uid       = grafana_folder.flink_cdc_poc.uid
  interval_seconds = 60

  # --- Monitor 1: Restart Loop ---
  # Mirrors rtdp-datadog-tf: flink.job.numRestarts change() > 3 per job
  rule {
    name      = "Flink Restart Loop"
    condition = "threshold"

    data {
      ref_id = "A"
      relative_time_range {
        from = 300
        to   = 0
      }
      datasource_uid = grafana_data_source.prometheus.uid
      model = jsonencode({
        expr         = "increase(flink_jobmanager_job_numRestarts[5m])"
        legendFormat = "{{job_name}}"
        refId        = "A"
      })
    }

    data {
      ref_id = "threshold"
      relative_time_range {
        from = 300
        to   = 0
      }
      datasource_uid = "__expr__"
      model = jsonencode({
        type       = "threshold"
        refId      = "threshold"
        expression = "A"
        conditions = [{
          evaluator = { params = [3], type = "gt" }
          operator  = { type = "and" }
          query     = { params = ["A"] }
          reducer   = { params = [], type = "last" }
          type      = "query"
        }]
      })
    }

    annotations = {
      summary       = "Flink job restart loop detected"
      description   = "Job {{$labels.job_name}} restarted more than 3 times in the last 5 minutes."
      __dashboardUid__ = "flink-cdc-poc-monitoring"
      __panelId__      = "1"
    }
    labels = { severity = "critical" }

    no_data_state  = "OK"
    exec_err_state = "Error"

    for = "1m"
  }

  # --- Monitor 2: Checkpoint Duration ---
  # Mirrors rtdp-datadog-tf: flink.jobmanager.job.lastCheckpointDuration > 180 s
  rule {
    name      = "Flink Checkpoint Duration High"
    condition = "threshold"

    data {
      ref_id = "A"
      relative_time_range {
        from = 300
        to   = 0
      }
      datasource_uid = grafana_data_source.prometheus.uid
      model = jsonencode({
        expr         = "flink_jobmanager_job_lastCheckpointDuration"
        legendFormat = "{{job_name}}"
        refId        = "A"
      })
    }

    data {
      ref_id = "threshold"
      relative_time_range {
        from = 300
        to   = 0
      }
      datasource_uid = "__expr__"
      model = jsonencode({
        type       = "threshold"
        refId      = "threshold"
        expression = "A"
        conditions = [{
          evaluator = { params = [180000], type = "gt" }
          operator  = { type = "and" }
          query     = { params = ["A"] }
          reducer   = { params = [], type = "last" }
          type      = "query"
        }]
      })
    }

    annotations = {
      summary          = "Flink checkpoint taking too long"
      description      = "Job {{$labels.job_name}} last checkpoint duration exceeded 180 s (value: {{$values.A}} ms)."
      __dashboardUid__ = "flink-cdc-poc-monitoring"
      __panelId__      = "2"
    }
    labels = { severity = "warning" }

    no_data_state  = "OK"
    exec_err_state = "Error"

    for = "2m"
  }

  # --- Monitor 3: Checkpoint Failures ---
  # Mirrors rtdp-datadog-tf: flink.jobmanager.job.numberOfFailedCheckpoints change() > 3
  rule {
    name      = "Flink Checkpoint Failures"
    condition = "threshold"

    data {
      ref_id = "A"
      relative_time_range {
        from = 300
        to   = 0
      }
      datasource_uid = grafana_data_source.prometheus.uid
      model = jsonencode({
        expr         = "increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m])"
        legendFormat = "{{job_name}}"
        refId        = "A"
      })
    }

    data {
      ref_id = "threshold"
      relative_time_range {
        from = 300
        to   = 0
      }
      datasource_uid = "__expr__"
      model = jsonencode({
        type       = "threshold"
        refId      = "threshold"
        expression = "A"
        conditions = [{
          evaluator = { params = [3], type = "gt" }
          operator  = { type = "and" }
          query     = { params = ["A"] }
          reducer   = { params = [], type = "last" }
          type      = "query"
        }]
      })
    }

    annotations = {
      summary          = "Flink checkpoint failures spike"
      description      = "Job {{$labels.job_name}} had more than 3 checkpoint failures in the last 5 minutes."
      __dashboardUid__ = "flink-cdc-poc-monitoring"
      __panelId__      = "3"
    }
    labels = { severity = "critical" }

    no_data_state  = "OK"
    exec_err_state = "Error"

    for = "1m"
  }

  depends_on = [grafana_data_source.prometheus]
}
