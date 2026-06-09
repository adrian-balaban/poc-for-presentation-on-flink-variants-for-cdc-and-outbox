package poc.common.config;

import java.util.function.Function;

public class JobConfig implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public final String mysqlHost;
    public final int    mysqlPort;
    public final String mysqlUser;
    public final String mysqlPassword;
    public final String mysqlDatabase;
    public final String mysqlTables;
    public final String kafkaBootstrap;
    public final String kafkaTopicPrefix;
    public final String serverId;

    private JobConfig(Builder b) {
        this.mysqlHost       = b.mysqlHost;
        this.mysqlPort       = b.mysqlPort;
        this.mysqlUser       = b.mysqlUser;
        this.mysqlPassword   = b.mysqlPassword;
        this.mysqlDatabase   = b.mysqlDatabase;
        this.mysqlTables     = b.mysqlTables;
        this.kafkaBootstrap  = b.kafkaBootstrap;
        this.kafkaTopicPrefix= b.kafkaTopicPrefix;
        this.serverId        = b.serverId;
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
            .serverId(env("MYSQL_SERVER_ID", "5900-5999", lookup))
            .build();
    }

    private static String env(String key, String defaultValue, Function<String, String> lookup) {
        String v = lookup.apply(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    public static class Builder {
        String mysqlHost = "localhost";
        int    mysqlPort = 3306;
        String mysqlUser, mysqlPassword, mysqlDatabase, mysqlTables;
        String kafkaBootstrap, kafkaTopicPrefix, serverId;

        public Builder mysqlHost(String v)        { this.mysqlHost = v;        return this; }
        public Builder mysqlPort(int v)            { this.mysqlPort = v;        return this; }
        public Builder mysqlUser(String v)         { this.mysqlUser = v;        return this; }
        public Builder mysqlPassword(String v)     { this.mysqlPassword = v;    return this; }
        public Builder mysqlDatabase(String v)     { this.mysqlDatabase = v;    return this; }
        public Builder mysqlTables(String v)       { this.mysqlTables = v;      return this; }
        public Builder kafkaBootstrap(String v)    { this.kafkaBootstrap = v;   return this; }
        public Builder kafkaTopicPrefix(String v)  { this.kafkaTopicPrefix = v; return this; }
        public Builder serverId(String v)          { this.serverId = v;         return this; }
        public JobConfig build()                   { return new JobConfig(this); }
    }
}
