package com.datastax.driver.examples.opentelemetry.demo;

import java.util.Map;

import com.datastax.driver.core.*;

import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.core.tracing.PrecisionLevel;
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

    private final PreparedStatement bumpUpPrepStmt;
    private final PreparedStatement bumpUpCheckPrepStmt;
    private final PreparedStatement queryBumpsStmt;
    private final PreparedStatement deleteBumpsStmt;
    private final PreparedStatement initBumpsStmt;

    public VisitsController() {
        cluster = Cluster.builder()
                .withoutJMXReporting()
                .addContactPoint("127.0.0.1")
                .build();

        openTelemetry = OpenTelemetryConfiguration.initializeForZipkin("127.0.0.1", 9411);

        tracer = openTelemetry.getTracer("test");
        openTelemetryTracingInfoFactory = new OpenTelemetryTracingInfoFactory(tracer, PrecisionLevel.FULL);
        cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);

        session = cluster.connect();

        /* Init database */
        Span initDatabaseSpan = tracer.spanBuilder("init_database").setSpanKind(SpanKind.SERVER).startSpan();
        try (Scope scope = initDatabaseSpan.makeCurrent()) {
            assert session != null;
            session.execute("DROP KEYSPACE IF EXISTS visits;");
            ResultSet result = session.execute(
                    "CREATE KEYSPACE visits WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
            //        assert result : "Database initialization failed";
            result = session.execute("CREATE TABLE visits.advertisement_rate (rate counter, id int PRIMARY KEY);");
            //        assert result : "Database initialization failed";

            bumpUpPrepStmt = session.prepare(
                    "UPDATE visits.advertisement_rate SET rate = rate + 1 WHERE id = ?;"
            );

            bumpUpCheckPrepStmt = session.prepare(
                    "SELECT id FROM visits.advertisement_rate WHERE id = ?;"
            );

            queryBumpsStmt = session.prepare("SELECT rate FROM visits.advertisement_rate WHERE id = ?;");

            deleteBumpsStmt = session.prepare("DELETE FROM visits.advertisement_rate WHERE id = ?;");

            initBumpsStmt = session.prepare("UPDATE visits.advertisement_rate SET rate = rate + 0 WHERE id = ?;");
        } finally {
            initDatabaseSpan.end();
        }
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
    @PostMapping("/bump_up/{rate_id}")
    public ResponseEntity<StatusResponse>
    bumpUp(@RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
           @RequestParam(value = "otel_tracing", defaultValue = "false") String opentelemetryTracing,
           @PathVariable("rate_id") int rateId) {
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
                    bumpUpSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/bump_up/" + rateId);

                    // Serve the request
                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doBumpUp(rateId, withClassicTracing);
                } finally {
                    bumpUpSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any OpenTelemetry span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doBumpUp(rateId, withClassicTracing);
        }
    }

    /* INTERNAL query_bumps(ad_id: int) → perform SELECT on that ad for Manager */
    @GetMapping("/query_bumps/{rate_id}")
    public ResponseEntity<Long>
    queryBumps(@RequestHeader Map<String, String> headers,
               @RequestParam(value = "classic_tracing", defaultValue = "false") final String classicTracing,
               @PathVariable("rate_id") int rateId) {
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
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/query_bumps/" + rateId);
                    // Serve the request

                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doQueryBumps(rateId, withClassicTracing);
                } finally {
                    queryBumpsSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doQueryBumps(rateId, withClassicTracing);
        }
    }

    /* INTERNAL init_ad(ad_id: int) → zero-initializes counter for ad_id */
    @PostMapping("/init_rate/{rate_id}")
    public ResponseEntity<StatusResponse>
    initRate(@RequestHeader Map<String, String> headers,
           @RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
           @PathVariable("rate_id") int rateId) {
        final boolean withClassicTracing = classicTracing.equals("true");

        Context extractedContext = getContextFromHeaders(headers);

        if (extractedContext != null) {
            try (Scope scope = extractedContext.makeCurrent()) {
                Span initRateSpan = tracer.spanBuilder("init_rate").setParent(extractedContext).startSpan();

                logger.debug("Created new span: " + initRateSpan.toString());
                try {
                    // Add the attributes defined in the Semantic Conventions
                    initRateSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "POST");
                    initRateSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    initRateSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    initRateSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/rate_id/" + rateId);
                    // Serve the request

                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doInitRate(rateId, withClassicTracing);
                } finally {
                    initRateSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doInitRate(rateId, withClassicTracing);
        }
    }

    /* INTERNAL init_ad(ad_id: int) → zero-initializes counter for ad_id */
    @PostMapping("/delete_rate/{rate_id}")
    public ResponseEntity<StatusResponse>
    deleteRate(@RequestHeader Map<String, String> headers,
              @RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
              @PathVariable("rate_id") int rateId) {
        final boolean withClassicTracing = classicTracing.equals("true");

        Context extractedContext = getContextFromHeaders(headers);

        if (extractedContext != null) {
            try (Scope scope = extractedContext.makeCurrent()) {
                Span deleteRateSpan = tracer.spanBuilder("delete_rate").setParent(extractedContext).startSpan();

                logger.debug("Created new span: " + deleteRateSpan.toString());
                try {
                    // Add the attributes defined in the Semantic Conventions
                    deleteRateSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "POST");
                    deleteRateSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    deleteRateSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8080");
                    deleteRateSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/rate_id/" + rateId);
                    // Serve the request

                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doDeleteRate(rateId, withClassicTracing);
                } finally {
                    deleteRateSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doDeleteRate(rateId, withClassicTracing);
        }
    }
//    List<String> fetchRequest(final boolean with_classic_tracing) {
//        Statement statement = new SimpleStatement("SELECT * FROM simplex.playlists;");
//        if (with_classic_tracing) {
//            statement.enableTracing();
//        }
//        ResultSet result = session.execute(statement);
//        List<String> results = new ArrayList<>();
//        for (Row row: result) {
//            results.add(row.toString());
//        }
//        if (with_classic_tracing)
//            results.add("CLASSIC_TRACING_DATA: " + result.getExecutionInfo().toString());
//        return results;
//    }

    private ResponseEntity<StatusResponse> doBumpUp(int rateId, final boolean withClassicTracing) {
        try {
            if (withClassicTracing) {
                bumpUpPrepStmt.enableTracing();
                bumpUpCheckPrepStmt.enableTracing();
            }
            BoundStatement bumpUpCheckStmt = bumpUpCheckPrepStmt.bind(rateId);
            ResultSet suchRateExists = session.execute(bumpUpCheckStmt);
            if (suchRateExists.isExhausted())
                return ResponseEntity.badRequest().body(new StatusResponse("error", "no such ad rate"));

            BoundStatement bumpUpStmt = bumpUpPrepStmt.bind(rateId);
            session.execute(bumpUpStmt);

            // Or using simple statement
//        session.execute("UPDATE advertisement_rate SET rate = rate + 1 WHERE id = 7;")

            return ResponseEntity.ok(StatusResponse.ok);
        } finally {
            bumpUpPrepStmt.disableTracing();
            bumpUpCheckPrepStmt.disableTracing();
        }
    }

    private ResponseEntity<Long> doQueryBumps(int rateId, final boolean withClassicTracing) {
        try {
            if (withClassicTracing) {
                queryBumpsStmt.enableTracing();
            }

            final BoundStatement bs = queryBumpsStmt.bind(rateId);
            final ResultSet rs = session.execute(bs);
            if (rs.isExhausted())
                return ResponseEntity.internalServerError().body(-1L);
            long rate = rs.one().getLong("rate");

            return ResponseEntity.ok(rate);
        } finally {
            queryBumpsStmt.disableTracing();
        }
    }

    private ResponseEntity<StatusResponse> doInitRate(int rateId, final boolean withClassicTracing) {
        try {
            if (withClassicTracing) {
                initBumpsStmt.enableTracing();
            }

            final BoundStatement initStmt = initBumpsStmt.bind(rateId);
            session.execute(initStmt);
            assert false;

            return ResponseEntity.ok(StatusResponse.ok);
        } finally {
            initBumpsStmt.disableTracing();
        }
    }

    private ResponseEntity<StatusResponse> doDeleteRate(int rateId, final boolean withClassicTracing) {
        try {
            if (withClassicTracing) {
                deleteBumpsStmt.enableTracing();
            }

            final BoundStatement deleteStmt = deleteBumpsStmt.bind(rateId);
            session.execute(deleteStmt);

            return ResponseEntity.ok(StatusResponse.ok);
        } finally {
            deleteBumpsStmt.disableTracing();
        }
    }
}
