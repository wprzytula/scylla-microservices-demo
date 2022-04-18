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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
        logger.debug("Extracted context: " + extractedContext.toString());

        if (!extractedContext.toString().equals("{}"))
            // if context is nonempty - unfortunately, opentelemetry java library has no native way for checking this
            return extractedContext;
        else
            return null;
    }


    /* MAPPINGS */

    /* EXTERNAL bump_up(ad_id: int) → bumps up related counter by 1 */
    @PostMapping(value = "/bump_up/{ad_id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StatusResponse>
    bumpUp(@RequestHeader Map<String, String> headers,
           @RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
           @RequestParam(value = "opentelemetry_tracing", defaultValue = "false") String opentelemetryTracing,
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
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "POST");
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/bump_up/" + adId);

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
    @GetMapping("/query_bumps/{advertiser}")
    public ResponseEntity<Map<Integer, Integer>>
    queryBumps(@RequestHeader Map<String, String> headers,
               @RequestParam(value = "classic_tracing", defaultValue = "false") final String classicTracing,
               @PathVariable("advertiser") String advertiser) {
        final boolean withClassicTracing = classicTracing.equals("true");

        Context extractedContext = getContextFromHeaders(headers);

        if (extractedContext != null) {
            try (Scope scope = extractedContext.makeCurrent()) {
                Span queryBumpsSpan = tracer.spanBuilder("query_bumps").setParent(extractedContext).startSpan();

                logger.debug("Created new span: " + queryBumpsSpan.toString());
                try {
                    // Add the attributes defined in the Semantic Conventions
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "GET");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/query_bumps/" + advertiser);
                    // Serve the request

                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doQueryBumps(advertiser, withClassicTracing);
                } finally {
                    queryBumpsSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doQueryBumps(advertiser, withClassicTracing);
        }
    }

    /* INTERNAL init_ad(ad_id: int) → zero-initializes counter for ad_id */
    @PostMapping(value = "/init_ad/{ad_id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StatusResponse>
    init_ad(@RequestHeader Map<String, String> headers,
            @RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
            @PathVariable("ad_id") int adId) {
        final boolean withClassicTracing = classicTracing.equals("true");

        Context extractedContext = getContextFromHeaders(headers);

        if (extractedContext != null) {
            try (Scope scope = extractedContext.makeCurrent()) {
                Span queryBumpsSpan = tracer.spanBuilder("init_ad").setParent(extractedContext).startSpan();

                logger.debug("Created new span: " + queryBumpsSpan.toString());
                try {
                    // Add the attributes defined in the Semantic Conventions
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "POST");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/ad_id" + adId);
                    // Serve the request

                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doInitAd(adId, withClassicTracing);
                } finally {
                    queryBumpsSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doInitAd(adId, withClassicTracing);
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

    private ResponseEntity<StatusResponse> doBumpUp(int adId, final boolean withClassicTracing) {
        return new ResponseEntity<>(new StatusResponse("error", "unimplemented"), HttpStatus.I_AM_A_TEAPOT);
    }

    private ResponseEntity<Map<Integer, Integer>> doQueryBumps(String advertiser, final boolean withClassicTracing) {
        return new ResponseEntity<>(new HashMap<>(), HttpStatus.NOT_IMPLEMENTED);
    }

    private ResponseEntity<StatusResponse> doInitAd(int adId, final boolean withClassicTracing) {
        return new ResponseEntity<>(new StatusResponse("error", "unimplemented"), HttpStatus.NOT_IMPLEMENTED);
    }
}
