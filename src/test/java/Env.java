import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.MountableFile;

/// The plumbing: real MySQL (row binlog is the default since 8.0), real Kafka, real
/// Debezium Connect, a consumer parked at the end of the topic. Nothing here is the
/// pattern; the pattern is in DebeziumOutboxTest.
class Env {

    static final String TOPIC = "order.events";
    static final Network NET = Network.newNetwork();

    static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4")
            .withNetwork(NET).withNetworkAliases("mysql")
            .withUsername("outbox").withPassword("outbox").withDatabaseName("outbox")
            .withCopyFileToContainer(MountableFile.forHostPath("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(NET).withNetworkAliases("kafka").withListener("kafka:19092");

    static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.3.2.Final")
            .withNetwork(NET).dependsOn(MYSQL, KAFKA)
            .withEnv(Map.of(
                    "BOOTSTRAP_SERVERS", "kafka:19092",
                    "GROUP_ID", "connect",
                    "CONFIG_STORAGE_TOPIC", "connect_configs",
                    "OFFSET_STORAGE_TOPIC", "connect_offsets",
                    "STATUS_STORAGE_TOPIC", "connect_statuses"))
            .withExposedPorts(8083)
            .waitingFor(Wait.forHttp("/connectors").forPort(8083));

    static Connection db;

    /// Three partitions so the "same aggregate, same partition" test means something.
    @BeforeAll
    static void start() throws Exception {
        Startables.deepStart(CONNECT).join();
        try (var admin = Admin.create(Map.of("bootstrap.servers", (Object) KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
        db = connect();
        registerConnector();
    }

    static Connection connect() throws Exception {
        return DriverManager.getConnection(MYSQL.getJdbcUrl(), "outbox", "outbox");
    }

    // --- kafka connect ------------------------------------------------------------

    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final String CONNECTOR = "/connectors/debezium-connector";

    static void registerConnector() throws Exception {
        var put = request(CONNECTOR + "/config")
                .header("content-type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(Files.readString(Path.of("connector.json")))).build();
        var res = HTTP.send(put, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("register: " + res.body());

        var running = "\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"";
        var status = request(CONNECTOR + "/status").GET().build();
        var deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        String body;
        do {
            Thread.sleep(500);
            body = HTTP.send(status, HttpResponse.BodyHandlers.ofString()).body();
        } while (!body.contains(running) && System.nanoTime() < deadline);
        if (!body.contains(running)) throw new IllegalStateException("task never started: " + body);
        System.out.println("  connect <- " + body);
        // RUNNING comes before the schema snapshot reads the binlog position; with
        // snapshot.mode=no_data anything written before that is never published.
        // The Binlog Dump thread appearing means streaming has started.
        awaitBinlogReaders("1");
    }

    static void deleteConnector() throws Exception {
        var res = HTTP.send(request(CONNECTOR).DELETE().build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("delete: " + res.body());
        // DELETE returns before the task stops; the Binlog Dump thread going away is the real signal
        awaitBinlogReaders("0");
    }

    /// Debezium shows up in MySQL as a replica: a thread running `Binlog Dump`. MySQL only
    /// notices a gone client when it tries to send something, so detaching takes up to one
    /// heartbeat (`connect.keep.alive.interval.ms` / 2 in connector.json).
    static final String BINLOG_READERS = "SELECT count(*) FROM performance_schema.processlist WHERE command LIKE 'Binlog Dump%'";

    static void awaitBinlogReaders(String count) throws Exception {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!count.equals(scalar(BINLOG_READERS)) && System.nanoTime() < deadline) {
            Thread.sleep(200);
        }
        if (!count.equals(scalar(BINLOG_READERS))) throw new IllegalStateException("binlog readers: expected " + count);
    }

    static HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + path));
    }

    // --- writing the way the application does --------------------------------------

    /// The orders row and its outbox row in ONE transaction. MySQL has no
    /// `WITH ... INSERT ... RETURNING`, so it is two statements between BEGIN and COMMIT.
    static String insertOrder(Connection c, String customer) throws Exception {
        var id = UUID.randomUUID().toString();
        inOneTransaction(c,
                "INSERT INTO orders (id, customer_id, amount_cents) VALUES ('%s', '%s', 1000)".formatted(id, customer),
                outboxRow(id, "order.created"));
        return id;
    }

    /// Same shape for a change to an existing order.
    static void updateOrder(Connection c, String orderId) throws Exception {
        inOneTransaction(c,
                "UPDATE orders SET amount_cents = 2000 WHERE id = '%s'".formatted(orderId),
                outboxRow(orderId, "order.updated"));
    }

    static String outboxRow(String orderId, String eventType) {
        return """
                INSERT INTO outbox (event_id, aggregate_id, event_type, payload)
                SELECT UUID(), id, '%s', JSON_OBJECT('id', id, 'customer_id', customer_id,
                                                     'amount_cents', amount_cents, 'created_at', created_at)
                FROM orders WHERE id = '%s'""".formatted(eventType, orderId);
    }

    /// Runs the statements in the caller's transaction if it has one open, otherwise
    /// opens one around them. Either way they commit or roll back together.
    static void inOneTransaction(Connection c, String... statements) throws Exception {
        var ownTx = c.getAutoCommit();
        if (ownTx) c.setAutoCommit(false);
        try (var s = c.createStatement()) {
            for (var sql : statements) s.execute(sql);
            if (ownTx) c.commit();
        } catch (Exception e) {
            if (ownTx) c.rollback();
            throw e;
        } finally {
            if (ownTx) c.setAutoCommit(true);
        }
    }

    static String scalar(String sql) throws Exception {
        return scalar(db, sql);
    }

    static String scalar(Connection c, String sql) throws Exception {
        var rs = c.createStatement().executeQuery(sql);
        rs.next();
        return rs.getString(1);
    }

    // --- reading the topic -----------------------------------------------------------

    static KafkaConsumer<String, String> topic;
    static final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    /// Empty tables, and a consumer parked at the end of the topic so each test
    /// reads back only what it produced.
    @BeforeEach
    void startClean() throws Exception {
        db.createStatement().execute("TRUNCATE orders");
        db.createStatement().execute("TRUNCATE outbox"); // resets AUTO_INCREMENT too
        var props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        received.clear();
        topic = new KafkaConsumer<>(props);
        var partitions = topic.partitionsFor(TOPIC).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        topic.assign(partitions);
        topic.seekToEnd(partitions);
        partitions.forEach(topic::position); // seekToEnd is lazy; force it before the test writes anything
    }

    @AfterEach
    void closeConsumer() {
        topic.close();
    }

    /// Everything published since the test started. Waits up to 15 s for at least
    /// `atLeast` records, then a second more so anything unexpected shows up too.
    static List<ConsumerRecord<String, String>> consume(int atLeast) {
        var deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (received.size() < atLeast && System.nanoTime() < deadline) {
            poll();
        }
        var grace = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < grace) {
            poll();
        }
        return received;
    }

    static void poll() {
        topic.poll(Duration.ofMillis(200)).forEach(r -> {
            received.add(r);
            System.out.printf("  topic <- partition=%d key=%s event_type=%s event_id=%s value=%s%n",
                    r.partition(), r.key(), header(r, "event_type"), header(r, "event_id"), r.value());
        });
    }

    static List<String> keys(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).toList();
    }

    static Set<String> partitions(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(r -> String.valueOf(r.partition())).collect(Collectors.toSet());
    }

    static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
