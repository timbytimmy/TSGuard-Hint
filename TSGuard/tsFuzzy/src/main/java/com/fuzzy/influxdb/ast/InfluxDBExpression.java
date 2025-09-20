package com.fuzzy.influxdb.ast;

public interface InfluxDBExpression {

    default InfluxDBConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

    default void checkSyntax() {}

}
