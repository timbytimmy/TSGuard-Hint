package com.fuzzy.influxdb.util;

public enum InfluxDBValueStateConstant {

    TIME_FIELD("time"),
    REF("ref"),
    ;

    private String value;

    InfluxDBValueStateConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

}
