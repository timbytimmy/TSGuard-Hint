package com.fuzzy.prometheus.ast;


import com.fuzzy.prometheus.PrometheusSchema;

public class PrometheusSchemaReference implements PrometheusExpression {

    private final PrometheusSchema schema;

    public PrometheusSchemaReference(PrometheusSchema schema) {
        this.schema = schema;
    }

    public PrometheusSchema getSchema() {
        return schema;
    }

}
