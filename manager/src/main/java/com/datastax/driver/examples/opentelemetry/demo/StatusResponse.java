package com.datastax.driver.examples.opentelemetry.demo;


public class StatusResponse {
    private final String status;
    private final String reason;

    public StatusResponse() { // for Jackson serialization/deserialization capability
        this.status = "";
        this.reason = "";
    }

    public StatusResponse(String status, String reason) {
        this.status = status;
        this.reason = reason;
    }

    public String getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public static StatusResponse ok = new StatusResponse("ok", "");
}
