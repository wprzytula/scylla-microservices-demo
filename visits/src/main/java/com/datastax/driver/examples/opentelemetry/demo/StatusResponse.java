package com.datastax.driver.examples.opentelemetry.demo;

public class StatusResponse {
    private final String status;
    private final String reason;

    public StatusResponse(String status, String reason) {
        this.status = status;
        this.reason = reason;
    }
}
