package com.fuzzy.influxdb.ast;

public class InfluxDBStringExpression implements InfluxDBExpression {

    private final String str;
    private final InfluxDBConstant expectedValue;

    public InfluxDBStringExpression(String str, InfluxDBConstant expectedValue) {
        this.str = str;
        this.expectedValue = expectedValue;
    }

    public String getStr() {
        return str;
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        return expectedValue;
    }

}
