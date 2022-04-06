package com.datastax.driver.examples.opentelemetry.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.Cluster;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.opentelemetry.OpenTelemetryTracingInfoFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    private final Cluster cluster;

    private final Tracer tracer;

    private final Session session;

    public GreetingController() {
        cluster = Cluster.builder()
            .withoutJMXReporting()
            .withClusterName("ZPP_telemetry")
            .addContactPoint("127.0.0.1")
            .build();

        OpenTelemetry openTelemetry = OpenTelemetryConfiguration.initializeForZipkin("localhost", 9411);

        tracer = openTelemetry.getTracer("test");
        cluster.setTracingInfoFactory(new OpenTelemetryTracingInfoFactory(tracer));

        session = cluster.connect();
    }


    @GetMapping("/fetch")
    public List<String> fetch(@RequestParam(value = "where", defaultValue = "") String name) {
        if (Span.current() != null) {
            return fetchRequest();
        }
        else {
            Span span = tracer.spanBuilder(name).startSpan();

            try (Scope scope = span.makeCurrent()) {
                return fetchRequest();
            }
            finally {
                span.end();
            }
        }
    }

    List<String> fetchRequest() {
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
}
