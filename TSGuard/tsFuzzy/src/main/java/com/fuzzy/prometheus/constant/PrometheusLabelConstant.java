package com.fuzzy.prometheus.constant;

public enum PrometheusLabelConstant {
    DATABASE("database"),
    DATABASE_INIT("databaseInit"),
    TABLE("table"),
    TIME_SERIES("timeSeries"),
    METRIC("__name__");

    private String label;

    PrometheusLabelConstant(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }
}
