package com.fuzzy.prometheus.ast;

import com.fuzzy.iotdb.ast.IotDBConstant;

public interface PrometheusExpression {

    default IotDBConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

    default void checkExpressionDataType() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
