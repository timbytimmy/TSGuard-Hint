package com.benchmark.influxdb.db;

public enum AggFunction {
    MAX("max"), MIN("min"), MEAN("mean"), SUM("sum"), COUNT("count"), MEDIAN("median"), STDDEV("stddev"),
    AVG("mean");

    private String func;

    AggFunction(String func) {
        this.func = func;
    }

    public String getFunc() {
        return func;
    }

    public void setFunc(String func) {
        this.func = func;
    }
}
