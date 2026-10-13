CREATE TABLE IF NOT EXISTS orders (
    id           CHAR(36)     PRIMARY KEY,
    customer_id  VARCHAR(255) NOT NULL,
    amount_cents BIGINT       NOT NULL CHECK (amount_cents > 0),
    created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

-- One row = one event decided but not yet published. Written in the same
-- transaction as the orders row; that is the whole pattern.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGINT       AUTO_INCREMENT PRIMARY KEY,
    event_id     CHAR(36)     NOT NULL UNIQUE,  -- survives redelivery; the consumer's dedup key
    aggregate_id VARCHAR(255) NOT NULL,         -- Kafka partition key
    event_type   VARCHAR(255) NOT NULL,
    payload      JSON         NOT NULL,
    published_at TIMESTAMP(6) NULL              -- NULL = queued
);

-- Debezium reads the binlog as if it were a replica, so the connector user needs the
-- replication privileges on top of the usual ones. PROCESS is only for the test that
-- looks at the Binlog Dump thread.
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, PROCESS ON *.* TO 'outbox'@'%';
