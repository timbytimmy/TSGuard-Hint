package com.benchmark.influxdb.db;

public enum Granularity {
    MINUTE("m"), HOUR("h"), DAY("d"), MONTH("mo"), YEAR("y");

    private String value;

    Granularity(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
