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
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class SenderController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    private final Cluster cluster;

    private final OpenTelemetryTracingInfoFactory tracingInfoFactory;

    private final OpenTelemetry openTelemetry;

    private final Tracer tracer;

    private final Session session;

    private final Logger logger = LoggerFactory.getLogger(SenderController.class);

    public SenderController() {
        cluster = Cluster.builder()
                .withoutJMXReporting()
                .withClusterName("ZPP_telemetry")
                .addContactPoint("127.0.0.1")
                .build();

        openTelemetry = OpenTelemetryConfiguration.initializeForZipkin("127.0.0.1", 9411);
        tracer = openTelemetry.getTracer("test");
        tracingInfoFactory = new OpenTelemetryTracingInfoFactory(tracer);
        cluster.setTracingInfoFactory(tracingInfoFactory);

        session = cluster.connect();
    }

    @Service
    static class Poker {


        private final RestTemplate restTemplate;

        public Poker() {
            restTemplate = new RestTemplate();
        }

        public String getPostsPlainJSON(final String classic_tracing) {
            final String url = "http://localhost:8080/fetch?classic_tracing=" + classic_tracing;
            return this.restTemplate.getForObject(url, String.class);
        }
    }
    @Service
    class PokerSpan {

        private final RestTemplate restTemplate;

        PokerSpan() {
            restTemplate = new RestTemplate();
        }

        public String getPostsPlainJSON(final String classic_tracing) {
            final String url = "http://localhost:8080/fetch?classic_tracing=" + classic_tracing;
            // Tell OpenTelemetry to inject the context in the HTTP headers
            final TextMapSetter<HttpHeaders> setter =
                    (carrier, key, value) -> {
                        // Insert the context as Header
                        assert carrier != null;
                        assert key != null;
                        carrier.add(key, value);
                        logger.info("Injecting context pair into header: key= " + key + ", value=" + value);
                    };

            Span outGoing = tracer.spanBuilder("poker_span").setSpanKind(SpanKind.CLIENT).startSpan();
            try (Scope scope = outGoing.makeCurrent()) {
                // Use the Semantic Conventions.
                // (Note that to set these, Span does not *need* to be the current instance in Context or Scope.)
                outGoing.setAttribute(SemanticAttributes.HTTP_METHOD, "GET");
                outGoing.setAttribute(SemanticAttributes.HTTP_URL, url.toString());

                HttpHeaders headers = new HttpHeaders();

                logger.info("Injecting context into headers: " + Context.current().toString());
                logger.info(openTelemetry.getPropagators().getTextMapPropagator().getClass().getName());
                openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), headers, setter);

                SpanContext spanContext = Span.fromContext(Context.current()).getSpanContext();
                if (!spanContext.isValid()) {
                    logger.warn("Invalid span context.");
                }

                logger.info("Injected context into headers: " + headers.toString());

                HttpEntity<String> request = new HttpEntity<>(headers);
                ResponseEntity<String> response = this.restTemplate.exchange(url, HttpMethod.GET, request, String.class);

                logger.info("SENDER: Request sent to receiver: " + response.toString());

                if (response.getStatusCode() == HttpStatus.OK) {
                    return response.getBody();
                } else {
                    return null;
                }
            } finally {
                outGoing.end();
            }
        }
    }

    @GetMapping("/poke")
    public String poke(@RequestParam(value = "classic_tracing", defaultValue = "false") String classic_tracing) {
        logger.debug("classic_tracing=" + classic_tracing);
        Poker s = new Poker();
        return s.getPostsPlainJSON(classic_tracing);
    }

    @GetMapping("/parent_span")
    public String parent_span(@RequestParam(value = "classic_tracing", defaultValue = "false") String classic_tracing) {
        PokerSpan s = new PokerSpan();
        return s.getPostsPlainJSON(classic_tracing);
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

    @GetMapping("/test_external_trace")
    public String test_external_trace() {
        return "";
    }
}
