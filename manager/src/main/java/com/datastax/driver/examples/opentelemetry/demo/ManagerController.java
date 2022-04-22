package com.datastax.driver.examples.opentelemetry.demo;

import com.datastax.driver.core.*;

import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class ManagerController {

    private static final String VISITS_URL = "http://localhost:8080";

    private final Cluster cluster;

    private final OpenTelemetryTracingInfoFactory openTelemetryTracingInfoFactory;

    private final NoopTracingInfoFactory noopTracingInfoFactory = new NoopTracingInfoFactory();

    private final OpenTelemetry openTelemetry;

    private final Tracer tracer;

    private final Session session;

    private final Logger logger = LoggerFactory.getLogger(ManagerController.class);

    private static final Map<String, Integer> durationOptions = new HashMap<String, Integer>(){{
        put("day", 4);
        put("week", 3);
        put("month", 2);
        put("year", 1);
    }};

    private final PreparedStatement createAdPrepStmt;
    private final PreparedStatement queryPricingPrepStmt;
    private final PreparedStatement decreaseBudgetPrepStmt;

    private final PreparedStatement queryRateIdPrepStmt;
    private final PreparedStatement queryAdsPrepStmt;
    private final PreparedStatement updateRateIdPrepStmt;

    private static int nextFreeAdId = 0;
    private static int nextFreeAdRateId = 0;

    // Tell OpenTelemetry to inject the context in the HTTP headers
    private final TextMapSetter<HttpHeaders> setter =
            (carrier, key, value) -> {
                // Insert the context as Header
                assert carrier != null;
                assert key != null;
                carrier.add(key, value);
                logger.info("Injecting context pair into header: key= " + key + ", value=" + value);
            };

    public ManagerController() {
        cluster = Cluster.builder()
                .withoutJMXReporting()
                .withClusterName("ZPP_telemetry")
                .addContactPoint("127.0.0.1")
                .build();

        openTelemetry = OpenTelemetryConfiguration.initializeForZipkin("127.0.0.1", 9411);
        tracer = openTelemetry.getTracer("test");
        openTelemetryTracingInfoFactory = new OpenTelemetryTracingInfoFactory(tracer);
        cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);

        session = cluster.connect();

        /* Init database */
        assert session != null;
        session.execute("DROP KEYSPACE IF EXISTS manager;");
        session.execute("CREATE KEYSPACE manager WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor' : 1};");
        session.execute("CREATE TABLE manager.advertiser (name text PRIMARY KEY, budget counter,);");
        session.execute(
                "CREATE TABLE manager.advertisement (id int, rate_id int, advertiser text, site text, active_to date," +
                        " PRIMARY KEY (advertiser, site, id));");
        session.execute("CREATE TABLE manager.site (name text PRIMARY KEY, pricing int);");
//        assert result : "Database initialization failed";

        // Prepare statements
//        activeToDatePrepStmt = session.prepare("SELECT currentDate() FROM system.local;");
        createAdPrepStmt = session.prepare(
                "INSERT INTO manager.advertisement (id, rate_id, advertiser, site, active_to) VALUES (?, ?, ?, ?, ?);");

        queryPricingPrepStmt = session.prepare("SELECT pricing FROM manager.site WHERE name = ?;");

        decreaseBudgetPrepStmt = session.prepare("UPDATE manager.advertiser SET budget = budget - ? WHERE name = ?;");

        queryAdsPrepStmt = session.prepare("SELECT * FROM manager.advertisement WHERE advertiser = ?;");

        queryRateIdPrepStmt = session.prepare(
                "SELECT rate_id FROM manager.advertisement WHERE advertiser = ? AND site = ? AND id = ?;");

        updateRateIdPrepStmt = session.prepare(
                "UPDATE manager.advertisement SET rate_id = ?" +
                " WHERE advertiser = ? AND site = ? AND id = ?;");

//        Just to play more easily with the microservices example:
        session.executeAsync("INSERT INTO site (name, pricing) VALUES ('google.com', 100);");
    }

    @Service
    class Visits {
        private final RestTemplate restTemplate = new RestTemplate();

        public <T> ResponseEntity<T> call(int rateId, boolean classicTracing, boolean opentelemetryTracing,
                                          String endpoint, HttpMethod httpMethod, Class<T> tClass) {
            final String url = VISITS_URL + "/" + endpoint + "/" + rateId + "?classic_tracing=" + classicTracing;
            if (opentelemetryTracing) {
                Span foreignCallSpan = tracer.spanBuilder(endpoint).setSpanKind(SpanKind.INTERNAL).startSpan();
                try (Scope scope = foreignCallSpan.makeCurrent()) {
                    foreignCallSpan.setAttribute(SemanticAttributes.HTTP_METHOD, httpMethod == HttpMethod.POST ? "POST" : "GET");
                    foreignCallSpan.setAttribute(SemanticAttributes.HTTP_URL, url);

                    HttpHeaders headers = new HttpHeaders();

                    logger.debug("Injecting context into headers: " + Context.current().toString());
                    openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), headers, setter);
                    logger.debug("Injected context into headers: " + headers);

                    HttpEntity<String> request = new HttpEntity<>(headers);
                    ResponseEntity<T> response = this.restTemplate.exchange(url, httpMethod, request, tClass);

                    logger.debug(String.format("Manager: %s request sent to Visits: %s", endpoint, response));

                    return response;
//                    if (response.getStatusCode() == HttpStatus.OK) {
//                        return response.getBody();
//                    } else {
//                        return null;
//                    }
                } finally {
                    foreignCallSpan.end();
                }
            } else {
                return restTemplate.exchange(url, httpMethod, new HttpEntity<>(""), tClass);
            }
        }

        public ResponseEntity<StatusResponse> initRate(int rateId, boolean classicTracing, boolean opentelemetryTracing) {
            return call(rateId, classicTracing, opentelemetryTracing, "init_rate", HttpMethod.POST, StatusResponse.class);
        }

        public ResponseEntity<Long> queryBumps(int rateId, boolean classicTracing, boolean opentelemetryTracing) {
            return call(rateId, classicTracing, opentelemetryTracing, "query_bumps", HttpMethod.GET, Long.class);
        }

        public ResponseEntity<StatusResponse> deleteRate(int rateId, boolean classicTracing, boolean opentelemetryTracing) {
            return call(rateId, classicTracing, opentelemetryTracing, "delete_rate", HttpMethod.POST, StatusResponse.class);
        }
    }

    private final Visits visits = new Visits();

    /* EXTERNAL create_ad(advertiser: String, site: String, active_to: String [”day”/”week”/”year”])
     → checks and reduces budget of advertiser, inserts the new ad into advertisement, and asks Visits
       for zero-initialization of rate entry for that ad. */
    @PostMapping("/create_ad")
    private ResponseEntity<StatusResponse>
    createAd(@RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
             @RequestParam(value = "otel_tracing", defaultValue = "false") String opentelemetryTracing,
             @RequestParam(value = "advertiser") String advertiser,
             @RequestParam(value = "site") String site,
             @RequestParam(value = "active_to") String activeTo /*[”day”/”week”/”month”/”year”]*/) {
        final boolean withClassicTracing = classicTracing.equals("true");
        final boolean withOpentelemetryTracing = opentelemetryTracing.equals("true");

        if (withOpentelemetryTracing) {
            Span createAdSpan = tracer.spanBuilder("create_ad").setSpanKind(SpanKind.SERVER).startSpan();
            try (Scope scope = createAdSpan.makeCurrent()) {
                logger.debug("Created new span: " + createAdSpan);
                try {
                    // Add the attributes defined in the Semantic Conventions
                    // TODO
                    createAdSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "POST");
                    createAdSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    createAdSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8081");
                    createAdSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/create_ad/");

                    // Serve the request
                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doCreateAd(advertiser, site, activeTo, withClassicTracing, withOpentelemetryTracing);
                } finally {
                    createAdSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any OpenTelemetry span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doCreateAd(advertiser, site, activeTo, withClassicTracing, withOpentelemetryTracing);
        }
    }

    /* EXTERNAL query_bumps(advertiser: String) → asks Visits iteratively for bumps of all ads for giver advertiser */
    @GetMapping(value = "/query_bumps/{advertiser}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity</*Map<Integer, Long>*/List<String>>
    queryBumps(@RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
           @RequestParam(value = "otel_tracing", defaultValue = "false") String opentelemetryTracing,
           @PathVariable("advertiser") String advertiser) {
        final boolean withClassicTracing = classicTracing.equals("true");
        final boolean withOpentelemetryTracing = opentelemetryTracing.equals("true");

        if (withOpentelemetryTracing) {
            Span queryBumpsSpan = tracer.spanBuilder("query_bumps").setSpanKind(SpanKind.SERVER).startSpan();
            try (Scope scope = queryBumpsSpan.makeCurrent()) {
                logger.debug("Created new span: " + queryBumpsSpan);
                try {
                    // Add the attributes defined in the Semantic Conventions
                    // TODO
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "GET");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8081");
                    queryBumpsSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/queryBumps/" + advertiser);

                    // Serve the request
                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doQueryBumps(advertiser, withClassicTracing, withOpentelemetryTracing);
                } finally {
                    queryBumpsSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any OpenTelemetry span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doQueryBumps(advertiser, withClassicTracing, withOpentelemetryTracing);
        }
    }

    /* EXTERNAL resetup_ad(advertiser: String, site: String, ad_id: int) → zero-initializes counter for ad_id */
    @PostMapping("/resetup_ad")
    private ResponseEntity<StatusResponse>
    resetupAd(@RequestParam(value = "classic_tracing", defaultValue = "false") String classicTracing,
              @RequestParam(value = "otel_tracing", defaultValue = "false") String opentelemetryTracing,
              @RequestParam(value = "advertiser") String advertiser,
              @RequestParam(value = "site") String site,
              @RequestParam(value = "ad_id") int adId) {
        final boolean withClassicTracing = classicTracing.equals("true");
        final boolean withOpentelemetryTracing = opentelemetryTracing.equals("true");

        if (withOpentelemetryTracing) {
            Span resetupAdSpan = tracer.spanBuilder("resetup_ad").setSpanKind(SpanKind.SERVER).startSpan();
            try (Scope scope = resetupAdSpan.makeCurrent()) {
                logger.debug("Created new span: " + resetupAdSpan);
                try {
                    // Add the attributes defined in the Semantic Conventions
                    // TODO
                    resetupAdSpan.setAttribute(SemanticAttributes.HTTP_METHOD, "POST");
                    resetupAdSpan.setAttribute(SemanticAttributes.HTTP_SCHEME, "http");
                    resetupAdSpan.setAttribute(SemanticAttributes.HTTP_HOST, "localhost:8081");
                    resetupAdSpan.setAttribute(SemanticAttributes.HTTP_TARGET, "/resetup_ad/");

                    // Serve the request
                    cluster.setTracingInfoFactory(openTelemetryTracingInfoFactory);
                    return doResetupAd(advertiser, site, adId, withClassicTracing, withOpentelemetryTracing);
                } finally {
                    resetupAdSpan.end();
                }
            }
        } else {
            logger.debug("Received no context, so proceeding without creating any OpenTelemetry span.");
            cluster.setTracingInfoFactory(noopTracingInfoFactory);
            return doResetupAd(advertiser, site, adId, withClassicTracing, withOpentelemetryTracing);
        }
    }

    private ResponseEntity<StatusResponse>
    doCreateAd(String advertiser, String site, String activeTo, boolean withClassicTracing, boolean withOpentelemetryTracing) {

        final Integer durationFieldIdx = durationOptions.get(activeTo);

        // Get proper statement accordingly to active_to value
        if (durationFieldIdx == null) {
            return new ResponseEntity<>(new StatusResponse("error", "unsupported active_to value" +
                    " (not any of <day>, <week>, <month>, <year>)"), HttpStatus.BAD_REQUEST);
        }

        // Get activeToDate
        final ResultSetFuture dateResultSetFuture = session.executeAsync("SELECT currentDate() AS now FROM system.local");
        // Get site's pricing
        final BoundStatement queryPricingStmt = queryPricingPrepStmt.bind(site);
        final ResultSetFuture pricingResultFuture = session.executeAsync(queryPricingStmt);

        LocalDate date = dateResultSetFuture.getUninterruptibly().one().getDate("now");
        date = date.add(durationFieldIdx, 1);
        // d.add(1, 1) = d + 1y
        // d.add(2, 1) = d + 1m
        // d.add(3, 1) = d + 1w
        // d.add(4, 1) = d + 1w

        final ResultSet pricingResult = pricingResultFuture.getUninterruptibly();
        if (pricingResult.isExhausted())
            return new ResponseEntity<>(new StatusResponse("error", "no such site in our pricing list"),
                    HttpStatus.BAD_REQUEST);
        final long pricing = pricingResult.one().getInt("pricing"); // widening cast to long

        // Decrease advertiser's budget
        @SuppressWarnings("PointlessArithmeticExpression")
        final BoundStatement decreaseBudgetStmt = decreaseBudgetPrepStmt.bind(pricing * 1, advertiser);
        session.executeAsync(decreaseBudgetStmt);

        final int adId = nextFreeAdId++;
        final int rateId = nextFreeAdRateId++;

        // Create new advertisement
        BoundStatement createOneXAdStmt = createAdPrepStmt.bind(adId, rateId, advertiser, site, date);
        session.executeAsync(createOneXAdStmt);

        // Initialize bumps

        return visits.initRate(rateId, withClassicTracing, withOpentelemetryTracing);
    }

    private ResponseEntity<List<String>>
    doQueryBumps(String advertiser, boolean withClassicTracing, boolean withOpentelemetryTracing) {
        List<String> adData = new ArrayList<>();

        BoundStatement queryAdsStmt = queryAdsPrepStmt.bind(advertiser);
        ResultSet ads = session.execute(queryAdsStmt);

        for (Row ad : ads) {
            final int adId = ad.getInt("id");
            final int rateId = ad.getInt("rate_id");
            final String site = ad.getString("site");
            final LocalDate activeTo = ad.getDate("active_to");

            ResponseEntity<Long> response = visits.queryBumps(rateId, withClassicTracing, withOpentelemetryTracing);

            if (response.getStatusCode() == HttpStatus.OK) {
                assert response.hasBody() : "No body in response with status code 200, while expected value of type Long";
                @SuppressWarnings("ConstantConditions")
                final long bumps = response.getBody();
                adData.add(String.format(
                        "ad_id=%d, advertiser=%s, site=%s, active_to=%s, rate_id=%d, bumps=%d",
                        adId, advertiser, site, activeTo.toString(), rateId, bumps));
            } else {
                logger.warn("Bumps query failed for ad_id = " + rateId);
            }
        }
        return ResponseEntity.ok(adData);
    }

    private ResponseEntity<StatusResponse> doResetupAd(String advertiser, String site, int adId, boolean withClassicTracing, boolean withOpentelemetryTracing) {
        final int rateId = nextFreeAdRateId++;

        // Remove rate associated with the old rate_id of this advertisement
        BoundStatement queryRateIdStmt = queryRateIdPrepStmt.bind(advertiser, site, adId);
        ResultSet rateIdResult = session.execute(queryRateIdStmt);
        if (rateIdResult.isExhausted()) {
            return ResponseEntity.badRequest().body(new StatusResponse("error", "no such ad"));
        }
        final int oldRateId = rateIdResult.one().getInt("rate_id");
        visits.deleteRate(oldRateId, withClassicTracing, withOpentelemetryTracing);

        // Update rate_id of this advertisement
        BoundStatement updateRateIdStmt = updateRateIdPrepStmt.bind(rateId, advertiser, site, adId);
        session.execute(updateRateIdStmt);

        // Initialize bumps
        visits.initRate(rateId, withClassicTracing, withOpentelemetryTracing);

        return ResponseEntity.ok(StatusResponse.ok);
    }
}
