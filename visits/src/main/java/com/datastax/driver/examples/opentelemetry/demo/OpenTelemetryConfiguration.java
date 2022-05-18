package com.datastax.driver.examples.opentelemetry.demo;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

/*
 * Copyright (C) 2021 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Example showing how to configure OpenTelemetry for tracing with Scylla Java Driver. */
public class OpenTelemetryConfiguration {
    private static final String SERVICE_NAME = "Visits microservice";

    public static OpenTelemetry initialize(SpanExporter spanExporter) {
        Resource serviceNameResource =
                Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, SERVICE_NAME));

        // Set to process the spans by the spanExporter.
        final SdkTracerProvider tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                        .setResource(Resource.getDefault().merge(serviceNameResource))
                        .build();
        OpenTelemetrySdk openTelemetry =
                OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                        .buildAndRegisterGlobal();

        // Add a shutdown hook to shut down the SDK.
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                tracerProvider::close));

        // Return the configured instance so it can be used for instrumentation.
        return openTelemetry;
    }

    public static OpenTelemetry initializeForZipkin(String ip, int port) {
        String endpointPath = "/api/v2/spans";
        String httpUrl = String.format("http://%s:%s", ip, port);

        SpanExporter exporter =
                ZipkinSpanExporter.builder().setEndpoint(httpUrl + endpointPath).build();

        return initialize(exporter);
    }
}
