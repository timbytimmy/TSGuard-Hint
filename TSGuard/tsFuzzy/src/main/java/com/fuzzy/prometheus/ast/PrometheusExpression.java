package com.fuzzy.prometheus.ast;

public interface PrometheusExpression {

    default PrometheusConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

    default void checkSyntax() {
    }

}
