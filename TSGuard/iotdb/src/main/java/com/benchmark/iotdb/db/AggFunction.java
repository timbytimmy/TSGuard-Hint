package com.benchmark.iotdb.db;

public enum AggFunction {
    MAX("MAX_VALUE"), MIN("MIN_VALUE"), MEAN("mean"), SUM("sum"), COUNT("COUNT"),
    MEDIAN("median"), STDDEV("stddev"), AVG("AVG"), RESULT_TYPE_VECTOR("vector"),
    RESULT_TYPE_MATRIX("matrix");

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
