package com.fuzzy.influxdb.ast;


import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;

public class InfluxDBTableReference implements InfluxDBExpression {

    private final InfluxDBTable table;

    public InfluxDBTableReference(InfluxDBTable table) {
        this.table = table;
    }

    public InfluxDBTable getTable() {
        return table;
    }

}
