package com.datastax.driver.examples.opentelemetry.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.*;

import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.opentelemetry.OpenTelemetryTracingInfoFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class VisitsController {

    private final Cluster cluster;
    private final Session session;

    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;
    private final OpenTelemetryTracingInfoFactory openTelemetryTracingInfoFactory;
    private final NoopTracingInfoFactory noopTracingInfoFactory = new NoopTracingInfoFactory();

    private final Logger logger = LoggerFactory.getLogger(VisitsController.class);

    public VisitsController() {
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

        if (!extractedContext.toString().equals("{}"))
            // if context is nonempty - unfortunately, opentelemetry java library has no native way for checking this
            return extractedContext;
        else
            return null;
    }

    /* MAPPINGS */


    /* EXTERNAL bump_up(ad_id: int) → bumps up related counter by 1 */
    @PostMapping(value = "/bump_up/{ad_id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public StatusResponse
    bumpUp(@RequestHeader Map<String, String> headers,
           @RequestParam(value = "classicTracing", defaultValue = "false") String classicTracing,
           @RequestParam(value = "opentelemetryTracing", defaultValue = "false") String opentelemetryTracing,
           @PathVariable("ad_id") int adId) {
        final boolean withClassicTracing = classicTracing.equals("true");
        final boolean withOpentelemetryTracing = opentelemetryTracing.equals("true");

        if (withOpentelemetryTracing) {
            Span bumpUpSpan = tracer.spanBuilder("bump_up").setSpanKind(SpanKind.SERVER).startSpan();
            try (Scope scope = bumpUpSpan.makeCurrent()) {
                logger.debug("Created new span: " + bumpUpSpan.toString());
                try {
                    // Add the attributes defined in the Semantic Conventions
                    // TODO
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "GET");
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/fetch");

                    // Serve the request
                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doBumpUp(adId, withClassicTracing);
                } finally {
                    bumpUpSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any OpenTelemetry span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doBumpUp(adId, withClassicTracing);
        }

    }

    /* INTERNAL query_bumps(ad_id: int) → perform SELECT on that ad for Manager */
    @GetMapping("/query_bumps")
    public Map<Integer, Integer>
    query_bumps(@RequestHeader Map<String, String> headers,
                @RequestParam(value = "classic_tracing", defaultValue = "false") String classic_tracing,
                @RequestParam(value = "opentelemetry_tracing", defaultValue = "false") String opentelemetry_tracing) {
        final boolean with_classic_tracing = classic_tracing.equals("true");
        final boolean with_opentelemetry_tracing = opentelemetry_tracing.equals("true");

        return new HashMap<>();
    }

    /* INTERNAL init_ad(ad_id: int) → zero-initializes counter for ad_id */
    @PostMapping(value = "/init_ad", produces = MediaType.APPLICATION_JSON_VALUE)
    public StatusResponse
    init_ad(@RequestHeader Map<String, String> headers,
            @RequestParam(value = "classic_tracing", defaultValue = "false") String classic_tracing,
            @RequestParam(value = "opentelemetry_tracing", defaultValue = "false") String opentelemetry_tracing) {
        final boolean with_classic_tracing = classic_tracing.equals("true");
        final boolean with_opentelemetry_tracing = opentelemetry_tracing.equals("true");


    }

    @GetMapping("/fetch")
    public List<String> fetch(@RequestHeader Map<String, String> headers,
                              @RequestParam(value = "classic_tracing", defaultValue = "false") String classic_tracing) {
        final boolean with_classic_tracing = classic_tracing.equals("true");

        Context extractedContext = getContextFromHeaders(headers);

        if (extractedContext != null) {
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

    private StatusResponse doBumpUp(int ad_id, final boolean with_classic_tracing) {

    }
}
