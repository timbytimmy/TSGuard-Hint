package com.fuzzy.prometheus.client.converter.series;

public enum SeriesTagKeyConstant {

    __NAME__("__name__"),
    DATABASE("database"),
    TABLE("table"),
    EXPORTED_JOB("exported_job");

    private String key;

    SeriesTagKeyConstant(String key) {
        this.key = key;
    }

    public String getKey() { return this.key; }

}
