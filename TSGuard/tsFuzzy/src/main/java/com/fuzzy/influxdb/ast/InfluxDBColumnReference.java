package com.fuzzy.influxdb.ast;


import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;

public class InfluxDBColumnReference implements InfluxDBExpression {

    private final InfluxDBColumn column;
    private final InfluxDBConstant value;

    public InfluxDBColumnReference(InfluxDBColumn column, InfluxDBConstant value) {
        this.column = column;
        this.value = value;
    }

    public static InfluxDBColumnReference create(InfluxDBColumn column, InfluxDBConstant value) {
        return new InfluxDBColumnReference(column, value);
    }

    public InfluxDBColumn getColumn() {
        return column;
    }

    public InfluxDBConstant getValue() {
        return value;
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        return value;
    }

}
