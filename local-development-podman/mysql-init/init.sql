-- poc_db schema used by all 5 variants

-- Grant binlog/CDC privileges to the flink user
-- RELOAD is required by Debezium for FLUSH TABLES WITH READ LOCK during snapshot
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'flink'@'%';
GRANT SELECT, LOCK TABLES ON poc_db.* TO 'flink'@'%';
FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS poc_db;
USE poc_db;

-- Used by: variant-1 (DataStream CDC), variant-2 (Table API), variant-3 (SQL API), variant-5 (YAML)
CREATE TABLE orders (
    id          BIGINT      NOT NULL AUTO_INCREMENT,
    customer_id BIGINT      NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    status      VARCHAR(1024),
    created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

-- Used by: variant-3 (SQL API — multi-table StatementSet demo)
CREATE TABLE customers (
    id    BIGINT      NOT NULL AUTO_INCREMENT,
    name  VARCHAR(128) NOT NULL,
    email VARCHAR(256) NOT NULL,
    PRIMARY KEY (id)
);

-- Used by: variant-4 (Outbox)
-- destination column drives per-row topic routing in OutboxRouter
CREATE TABLE outbox_events (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    destination VARCHAR(128) NOT NULL,
    payload     JSON         NOT NULL,
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

-- ── Seed data ──────────────────────────────────────────────────────────────

INSERT INTO customers (name, email) VALUES
    ('Alice',   'alice@example.com'),
    ('Bob',     'bob@example.com'),
    ('Charlie', 'charlie@example.com');

INSERT INTO orders (customer_id, amount, status) VALUES
    (1, 99.99,  'PENDING'),
    (2, 249.50, 'SHIPPED'),
    (3, 15.00,  'DELIVERED');

INSERT INTO outbox_events (destination, payload) VALUES
    ('payments',      '{"order_id":1,"amount":99.99}'),
    ('notifications', '{"user_id":2,"message":"Order shipped"}'),
    ('audit',         '{"action":"CREATE","entity":"order","id":3}');
