# outbox-debezium-mysql

Transactional outbox drained by Debezium, MySQL edition of
[outbox-debezium](https://github.com/machado-vitor/outbox-debezium). `orders` row and
`outbox` row are written in one transaction; Debezium reads the MySQL binlog as if it were
a replica (`binlog_format=ROW`, the default since 8.0) and the `EventRouter` SMT turns each
`outbox` INSERT into a record: key = `aggregate_id`, value = `payload`, headers
`event_id` / `event_type`. No code, one JSON (`connector.json`).

```sh
mvn test
```

That is the whole demo. `DebeziumOutboxTest` (infrastructure in `Env`) starts a real MySQL 8.4,
a real Kafka and a real Debezium Connect (Testcontainers), registers `connector.json`,
writes rows the way an application would, and prints what reached the topic:

| test | shows |
|---|---|
| `publishesTheOrderEventWithKeyHeadersAndPayload` | key = `aggregate_id`, headers `event_id` / `event_type`, value = payload. `published_at` stays NULL: Debezium never touches the table, the binlog position is the mark |
| `aRolledBackTransactionPublishesNothing` | the outbox row lives in the business transaction, so a rollback takes the event with it. The binlog holds commits, not writes |
| `aRowThatBecomesVisibleLateIsStillPublished` | tx A takes the lower id but commits after tx B. The table is in id order, the topic is in commit order. Nothing is skipped |
| `eventsOfOneAggregateShareAPartitionInOrder` | key = `aggregate_id`, so every event of one order lands on one partition, in order |
| `rowsWrittenWhileTheConnectorIsDownArePublishedWhenItReturns` | Kafka Connect keeps the binlog position in `connect_offsets`; rows written in the gap are published on return |

## What changes from Postgres

| | Postgres | MySQL |
|---|---|---|
| change log | WAL, needs `wal_level=logical` | binlog, on by default (`binlog_format=ROW`) |
| how Debezium attaches | replication slot + publication (`pgoutput`) | as a replica: `database.server.id`, shows up as a `Binlog Dump` thread |
| where the position lives | the slot, inside Postgres | `connect_offsets` topic, inside Kafka |
| what holds the log while the connector is down | the slot pins the WAL forever (disk grows until you drop it) | nothing: the binlog expires after `binlog_expire_logs_seconds` (30 days). A longer outage loses events |
| schema tracking | not needed | `schema.history.internal.kafka.*` topic, Debezium replays DDL from it on restart |
| privileges | `REPLICATION` role | `SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT` (`schema.sql`) |
| one atomic write | `WITH o AS (INSERT ... RETURNING *) INSERT INTO outbox ... SELECT FROM o`, one statement | no `RETURNING`: two statements between BEGIN and COMMIT (`Env.inOneTransaction`) |

Notes: `snapshot.mode=no_data` means rows that existed before the connector streams are
not published; the task reports RUNNING about a second before it reads the binlog
position, so `Env` waits for the `Binlog Dump` thread before writing. MySQL only notices
a gone client when it tries to send, so `connect.keep.alive.interval.ms=2000` makes the
server heartbeat every second and the dump thread go away promptly after the connector is
deleted; the default would be 30 s.

Siblings: [outbox-relay](https://github.com/machado-vitor/outbox-relay) (own poller, same table) and
[outbox-jdbc-source](https://github.com/machado-vitor/outbox-jdbc-source) (the Kafka Connect poller that loses events).
Compared side by side in [outbox-compared](https://github.com/machado-vitor/outbox-compared).
