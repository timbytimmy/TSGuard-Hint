package com.fuzzy.prometheus.client.builder;

import java.net.URI;

public class CleanTombstonesBuilder implements QueryBuilder {
    private static final String TARGET_URI = "/api/v1/admin/tsdb/clean_tombstones";

    private String serverUrl;

    public CleanTombstonesBuilder(String serverUrl) {
        this.serverUrl = serverUrl + TARGET_URI;
    }

    public URI build() {
        return URI.create(serverUrl);
    }

    private boolean validate() {
        return true;
    }
}
