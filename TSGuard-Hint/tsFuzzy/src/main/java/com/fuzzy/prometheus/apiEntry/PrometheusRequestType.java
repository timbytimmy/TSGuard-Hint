package com.fuzzy.prometheus.apiEntry;

public enum PrometheusRequestType {
    instant_query,
    range_query,
    series_query,
    push_gateway_series_query,
    series_delete,
    push_gateway_series_delete,
    push_data;

    public boolean isPushData() {
        switch (this) {
            case push_data:
                return true;
            case series_delete:
            case series_query:
            case range_query:
            case instant_query:
            case push_gateway_series_query:
            default:
                return false;
        }
    }
}
