package com.fuzzy.prometheus.ast;


import com.fuzzy.prometheus.PrometheusSchema;

public class PrometheusTableReference implements PrometheusExpression {

    private final PrometheusSchema.PrometheusTable table;

    public PrometheusTableReference(PrometheusSchema.PrometheusTable table) {
        this.table = table;
    }

    public PrometheusSchema.PrometheusTable getTable() {
        return table;
    }

}
