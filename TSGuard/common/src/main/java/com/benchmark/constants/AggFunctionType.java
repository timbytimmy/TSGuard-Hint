package com.benchmark.constants;

public enum AggFunctionType {
    MAX("MAX"), MIN("MIN"), COUNT("COUNT"), AVG("AVG");

    private String func;

    AggFunctionType(String func) {
        this.func = func;
    }

    public String getFunc() {
        return func;
    }

    public void setFunc(String func) {
        this.func = func;
    }
}
