package com.datastax.driver.examples.opentelemetry.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.*;

import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.opentelemetry.OpenTelemetryTracingInfoFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReceiverController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    private final Cluster cluster;
    private final Session session;

    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;
    private final OpenTelemetryTracingInfoFactory openTelemetryTracingInfoFactory;
    private final NoopTracingInfoFactory noopTracingInfoFactory = new NoopTracingInfoFactory();

    private final Logger logger = LoggerFactory.getLogger(ReceiverController.class);

    public ReceiverController() {
        cluster = Cluster.builder()
                .withoutJMXReporting()
                .withClusterName("ZPP_telemetry")
                .addContactPoint("127.0.0.1")
                .build();

        openTelemetry = OpenTelemetryConfiguration.initializeForZipkin("127.0.0.1", 9411);

        tracer = openTelemetry.getTracer("test");
        openTelemetryTracingInfoFactory = new OpenTelemetryTracingInfoFactory(tracer);

        session = cluster.connect();
    }


    private Context getContextFromHeaders(Map<String, String> headers) {
        TextMapGetter<Map<String, String>> getter =
                new TextMapGetter<Map<String, String>>() {
                    @Override
                    public String get(Map<String, String> carrier, String key) {
                        assert carrier != null;
                        if (carrier.containsKey(key)) {
                            return carrier.get(key);
                        }
                        return null;
                    }

                    @Override
                    public Iterable<String> keys(Map<String, String> carrier) {
                        return carrier.keySet();
                    }
                };

        // Extract the SpanContext and other elements from the request.
        Context extractedContext = openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, getter);
        logger.info("Extracted context: " + extractedContext.toString());
        return extractedContext;
    }

    @GetMapping("/fetch")
    public List<String> fetch(@RequestHeader Map<String, String> headers,
                              @RequestParam(value = "classic_tracing", defaultValue = "false") String classic_tracing) {
        final boolean with_classic_tracing = classic_tracing.equals("true");

        Context extractedContext = getContextFromHeaders(headers);

        if (!extractedContext.toString().equals("{}")) {
            try (Scope scope = extractedContext.makeCurrent()) {
                // Automatically use the extracted SpanContext as parent.
                Span serverSpan = tracer.spanBuilder("fetch").setParent(extractedContext).startSpan();

                logger.info("Created new span: " + serverSpan.toString());
                try {
                    // Add the attributes defined in the Semantic Conventions
                    serverSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "GET");
                    serverSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    serverSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    serverSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/fetch");
                    // Serve the request

                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return fetchRequest(with_classic_tracing);
                } finally {
                    serverSpan.end();
                }
            }
        } else {
            logger.info("Received no context, so proceeding without creating any span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return fetchRequest(with_classic_tracing);
        }
    }

    List<String> fetchRequest(final boolean with_classic_tracing) {
        Statement statement = new SimpleStatement("SELECT * FROM simplex.playlists;");
        if (with_classic_tracing) {
            statement.enableTracing();
        }
        ResultSet result = session.execute(statement);
        List<String> results = new ArrayList<>();
        for (Row row: result) {
            results.add(row.toString());
        }
        if (with_classic_tracing)
            results.add("CLASSIC_TRACING_DATA: " + result.getExecutionInfo().toString());
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
