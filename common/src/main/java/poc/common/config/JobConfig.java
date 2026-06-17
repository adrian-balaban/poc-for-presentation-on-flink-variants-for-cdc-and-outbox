package poc.common.config;

import java.util.function.Function;

public class JobConfig implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  // Default per-variant server-ID ranges (mirror the table in CLAUDE.md). Referenced
  // by both the Builder field initialisers and the fromEnv() env-lookup fallbacks so
  // the two code paths can never drift apart.
  static final String DEFAULT_SERVER_ID = "5900-5999";
  static final String DEFAULT_OUTBOX_SERVER_ID = "5600-5699";
  static final String DEFAULT_TABLE_API_SERVER_ID = "6000-6099";
  static final String DEFAULT_SQL_API_ORDERS_SERVER_ID = "5800-5849";
  static final String DEFAULT_SQL_API_CUSTOMERS_SERVER_ID = "5850-5899";

  // Default S3 checkpoint directories per variant. Each variant uses distinct directories
  // to avoid state collision when multiple jobs run concurrently.
  static final String DEFAULT_CHECKPOINT_DIR = "s3://flink-checkpoints/checkpoints";
  static final String DEFAULT_SAVEPOINT_DIR = "s3://flink-checkpoints/savepoints";

  public final String mysqlHost;
  public final int mysqlPort;
  public final String mysqlUser;
  public final String mysqlPassword;
  public final String mysqlDatabase;
  public final String mysqlTables;
  public final String kafkaBootstrap;
  public final String kafkaTopicPrefix;
  public final String serverId;
  public final String outboxServerId;
  public final String tableApiServerId;
  public final String sqlApiOrdersServerId;
  public final String sqlApiCustomersServerId;
  public final String checkpointDir;
  public final String savepointDir;

  private JobConfig(Builder b) {
    this.mysqlHost = b.mysqlHost;
    this.mysqlPort = b.mysqlPort;
    this.mysqlUser = b.mysqlUser;
    this.mysqlPassword = b.mysqlPassword;
    this.mysqlDatabase = b.mysqlDatabase;
    this.mysqlTables = b.mysqlTables;
    this.kafkaBootstrap = b.kafkaBootstrap;
    this.kafkaTopicPrefix = b.kafkaTopicPrefix;
    this.serverId = b.serverId;
    this.outboxServerId = b.outboxServerId;
    this.tableApiServerId = b.tableApiServerId;
    this.sqlApiOrdersServerId = b.sqlApiOrdersServerId;
    this.sqlApiCustomersServerId = b.sqlApiCustomersServerId;
    this.checkpointDir = b.checkpointDir;
    this.savepointDir = b.savepointDir;
  }

  public static JobConfig fromEnv() {
    return fromEnv(System::getenv);
  }

  // NOTE: default credentials below are POC-only for local Docker Compose.
  // In production always supply MYSQL_USER / MYSQL_PASSWORD via a secrets manager
  // or Kubernetes Secrets — never rely on these defaults outside the dev environment.
  static JobConfig fromEnv(Function<String, String> lookup) {
    return new Builder()
        .mysqlHost(env("MYSQL_HOST", "localhost", lookup))
        .mysqlPort(Integer.parseInt(env("MYSQL_PORT", "3306", lookup)))
        .mysqlUser(env("MYSQL_USER", "flink", lookup))
        .mysqlPassword(env("MYSQL_PASSWORD", "flink", lookup))
        .mysqlDatabase(env("MYSQL_DATABASE", "poc_db", lookup))
        .mysqlTables(env("MYSQL_TABLES", "poc_db.orders", lookup))
        .kafkaBootstrap(env("KAFKA_BOOTSTRAP", "localhost:9092", lookup))
        .kafkaTopicPrefix(env("KAFKA_TOPIC_PREFIX", "poc.cdc", lookup))
        .serverId(env("MYSQL_SERVER_ID", DEFAULT_SERVER_ID, lookup))
        .outboxServerId(env("MYSQL_OUTBOX_SERVER_ID", DEFAULT_OUTBOX_SERVER_ID, lookup))
        .tableApiServerId(env("MYSQL_TABLE_API_SERVER_ID", DEFAULT_TABLE_API_SERVER_ID, lookup))
        .sqlApiOrdersServerId(
            env("MYSQL_SQL_API_ORDERS_SERVER_ID", DEFAULT_SQL_API_ORDERS_SERVER_ID, lookup))
        .sqlApiCustomersServerId(
            env("MYSQL_SQL_API_CUSTOMERS_SERVER_ID", DEFAULT_SQL_API_CUSTOMERS_SERVER_ID, lookup))
        .checkpointDir(env("FLINK_CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR, lookup))
        .savepointDir(env("FLINK_SAVEPOINT_DIR", DEFAULT_SAVEPOINT_DIR, lookup))
        .build();
  }

  private static String env(String key, String defaultValue, Function<String, String> lookup) {
    String v = lookup.apply(key);
    return (v != null && !v.isBlank()) ? v : defaultValue;
  }

  public static class Builder {
    String mysqlHost = "localhost";
    int mysqlPort = 3306;
    String mysqlUser, mysqlPassword, mysqlDatabase, mysqlTables;
    String kafkaBootstrap, kafkaTopicPrefix;
    String serverId = DEFAULT_SERVER_ID;
    String outboxServerId = DEFAULT_OUTBOX_SERVER_ID;
    String tableApiServerId = DEFAULT_TABLE_API_SERVER_ID;
    String sqlApiOrdersServerId = DEFAULT_SQL_API_ORDERS_SERVER_ID;
    String sqlApiCustomersServerId = DEFAULT_SQL_API_CUSTOMERS_SERVER_ID;
    String checkpointDir = DEFAULT_CHECKPOINT_DIR;
    String savepointDir = DEFAULT_SAVEPOINT_DIR;

    public Builder mysqlHost(String v) {
      this.mysqlHost = v;
      return this;
    }

    public Builder mysqlPort(int v) {
      this.mysqlPort = v;
      return this;
    }

    public Builder mysqlUser(String v) {
      this.mysqlUser = v;
      return this;
    }

    public Builder mysqlPassword(String v) {
      this.mysqlPassword = v;
      return this;
    }

    public Builder mysqlDatabase(String v) {
      this.mysqlDatabase = v;
      return this;
    }

    public Builder mysqlTables(String v) {
      this.mysqlTables = v;
      return this;
    }

    public Builder kafkaBootstrap(String v) {
      this.kafkaBootstrap = v;
      return this;
    }

    public Builder kafkaTopicPrefix(String v) {
      this.kafkaTopicPrefix = v;
      return this;
    }

    public Builder serverId(String v) {
      this.serverId = v;
      return this;
    }

    public Builder outboxServerId(String v) {
      this.outboxServerId = v;
      return this;
    }

    public Builder tableApiServerId(String v) {
      this.tableApiServerId = v;
      return this;
    }

    public Builder sqlApiOrdersServerId(String v) {
      this.sqlApiOrdersServerId = v;
      return this;
    }

    public Builder sqlApiCustomersServerId(String v) {
      this.sqlApiCustomersServerId = v;
      return this;
    }

    public Builder checkpointDir(String v) {
      this.checkpointDir = v;
      return this;
    }

    public Builder savepointDir(String v) {
      this.savepointDir = v;
      return this;
    }

    public JobConfig build() {
      if (mysqlHost == null || mysqlHost.isBlank())
        throw new IllegalStateException("mysqlHost is required");
      if (mysqlUser == null || mysqlUser.isBlank())
        throw new IllegalStateException("mysqlUser is required");
      if (mysqlPassword == null) throw new IllegalStateException("mysqlPassword is required");
      if (mysqlDatabase == null || mysqlDatabase.isBlank())
        throw new IllegalStateException("mysqlDatabase is required");
      if (mysqlTables == null || mysqlTables.isBlank())
        throw new IllegalStateException("mysqlTables is required");
      if (kafkaBootstrap == null || kafkaBootstrap.isBlank())
        throw new IllegalStateException("kafkaBootstrap is required");
      if (kafkaTopicPrefix == null || kafkaTopicPrefix.isBlank())
        throw new IllegalStateException("kafkaTopicPrefix is required");
      if (serverId == null || serverId.isBlank())
        throw new IllegalStateException("serverId is required");
      if (outboxServerId == null || outboxServerId.isBlank())
        throw new IllegalStateException("outboxServerId is required");
      if (tableApiServerId == null || tableApiServerId.isBlank())
        throw new IllegalStateException("tableApiServerId is required");
      if (sqlApiOrdersServerId == null || sqlApiOrdersServerId.isBlank())
        throw new IllegalStateException("sqlApiOrdersServerId is required");
      if (sqlApiCustomersServerId == null || sqlApiCustomersServerId.isBlank())
        throw new IllegalStateException("sqlApiCustomersServerId is required");
      return new JobConfig(this);
    }
  }
}
