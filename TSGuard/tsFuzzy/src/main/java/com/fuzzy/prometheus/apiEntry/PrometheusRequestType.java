package com.fuzzy.prometheus.apiEntry;

public enum PrometheusRequestType {
    INSTANT_QUERY,
    RANGE_QUERY,
    SERIES_QUERY,
    PUSH_GATEWAY_SERIES_QUERY,
    SERIES_DELETE,
    PUSH_GATEWAY_SERIES_DELETE,
    PUSH_DATA;

    public boolean isPushData() {
        switch (this) {
            case PUSH_DATA:
                return true;
            case SERIES_DELETE:
            case SERIES_QUERY:
            case RANGE_QUERY:
            case INSTANT_QUERY:
            case PUSH_GATEWAY_SERIES_QUERY:
            default:
                return false;
        }
    }
}
