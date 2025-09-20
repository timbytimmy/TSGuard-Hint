package com.fuzzy.prometheus.client.builder.pushGateway;

import com.fuzzy.prometheus.client.builder.QueryBuilder;

import java.net.URI;

public class PushGatewaySeriesQueryBuilder implements QueryBuilder {
    private static final String TARGET_URI = "/api/v1/metrics";

    private String serverUrl;

    public PushGatewaySeriesQueryBuilder(String serverUrl) {
        this.serverUrl = serverUrl + TARGET_URI;
    }

    public URI build() {
        return URI.create(serverUrl);
    }

    private boolean validate() {
        return true;
    }
}
