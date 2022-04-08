package com.datastax.driver.examples.opentelemetry.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.Cluster;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.opentelemetry.OpenTelemetryTracingInfoFactory;
import io.opentelemetry.api.OpenTelemetry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Random;

@RestController
public class GreetingController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    private final Cluster cluster;
    private final Cluster rawCluster;

    private final OpenTelemetryTracingInfoFactory tracingInfoFactory;

    private final Session session;
    private final Session rawSession;

    public GreetingController() {
        cluster = Cluster.builder()
            .withoutJMXReporting()
            .withClusterName("ZPP_telemetry")
            .addContactPoint("127.0.0.1")
            .build();

        rawCluster = Cluster.builder()
                .withoutJMXReporting()
                .withClusterName("ZPP_telemetry")
                .addContactPoint("127.0.0.1")
                .build();

        rawSession = rawCluster.connect();

        OpenTelemetry openTelemetry = OpenTelemetryConfiguration.initializeForZipkin("localhost", 9411);

        tracingInfoFactory = new OpenTelemetryTracingInfoFactory(openTelemetry.getTracer("test"));
        cluster.setTracingInfoFactory(tracingInfoFactory);

        session = cluster.connect();
    }


    @GetMapping("/fetch")
    public List<String> fetch(@RequestParam(value = "where", defaultValue = "") String name) {
        ResultSet result = session.execute("SELECT * FROM simplex.playlists;");
        List<String> results = new ArrayList<>();
        for (Row row: result) {
            results.add(row.toString());
        }
        return results;
    }

    @GetMapping("/create")
    public void create() {
        session.execute("" +
                "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication = " +
                "{'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.playlists ("
                        + "id uuid,"
                        + "title text,"
                        + "album text, "
                        + "artist text,"
                        + "song_id uuid,"
                        + "PRIMARY KEY (id, title, album, artist)"
                        + ");");
        session.executeAsync(
                "CREATE TABLE IF NOT EXISTS simplex.songs ("
                        + "id uuid,"
                        + "title text,"
                        + "album text,"
                        + "artist text,"
                        + "tags set<text>,"
                        + "data blob,"
                        + "PRIMARY KEY ((title, artist), album)"
                        + ");");
        session.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                "VALUES (2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,756716f7-2e54-4715-9f00-91dcbea6cf50," +
                " 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker');");
    }

    @GetMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        return new Greeting(counter.incrementAndGet(), String.format(template, name));
    }

    private String randomString() {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder stringBuilder = new StringBuilder();
        Random random = new Random();

        for(int i = 0; i < 5; ++i) {
            int index = random.nextInt(alphabet.length());
            char c = alphabet.charAt(index);
            stringBuilder.append(c);
        }

        return stringBuilder.toString();
    }

    private void insertValuesBottleneck() {
        String name = "";

        rawSession.execute("INSERT INTO site (name, pricing) VALUES ('scylladb.com', 10);");
        for (int i = 0; i < 10; ++i) {
            name = randomString();
            rawSession.execute("UPDATE advertiser SET budget = budget + 1000 WHERE name = '" + name + "';");
            rawSession.execute("INSERT INTO advertisement (id, advertiser, site, active_to) VALUES (uuid(), '" + name + "', 'scylladb.com', TODATE(now()));");
        }

        for (int j = 1; j < 1e6; ) {
            BatchStatement statement = new BatchStatement();
            for (int i = 0; i < 65535 && j < 1e6; ++i, ++j) {
                statement.add(new SimpleStatement("INSERT INTO advertisement (id, advertiser, site, active_to) VALUES (uuid(), '" + name + "', 'scylladb.com', TODATE(now()));"));
            }
            rawSession.execute(statement);
        }
    }

    @GetMapping("/prepareBottleneck")
    private void prepareBottleneck() {
        rawSession.execute("DROP KEYSPACE IF EXISTS advertisementService;");
        rawSession.execute("CREATE KEYSPACE advertisementService WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");

        rawSession.execute("USE advertisementService;");
        session.execute("USE advertisementService;");

        rawSession.execute("CREATE TABLE advertiser (" +
                "name text PRIMARY KEY," +
                "budget counter," +
                ");");
        rawSession.execute("CREATE TABLE advertisement (" +
                "id uuid," +
                "advertiser text," +
                "site text," +
                "active_to date," +
                "PRIMARY KEY (advertiser, site, id) " +
                ");");
        rawSession.execute("CREATE TABLE advertisement_rate (" +
                "    rate counter," +
                "    id uuid PRIMARY KEY" +
                ");");
        rawSession.execute("CREATE TABLE site (" +
                "name text PRIMARY KEY," +
                "pricing int" +
                ");");

        insertValuesBottleneck();
    }

    @GetMapping("/bottleneck/classic")
    public void bottleneckClassic() {
        rawSession.execute("USE advertisementService;");
        ResultSet rs = rawSession.execute("SELECT name FROM advertiser;");
        for (Row name : rs) {
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM advertisement WHERE advertiser = '" + name.get(0, String.class) + "';");
            stmt.enableTracing();
            rawSession.execute(stmt);
        }
    }

    @GetMapping("/bottleneck/opentelemetry")
    public void bottleneckOpenTelemetry() {
        session.execute("USE advertisementService;");
        BatchStatement statement = new BatchStatement();
        ResultSet rs = session.execute("SELECT name FROM advertiser;");
        for (Row name : rs) {
            session.execute("SELECT * FROM advertisement WHERE advertiser = '" + name.get(0, String.class) + "';");
        }
    }

}
