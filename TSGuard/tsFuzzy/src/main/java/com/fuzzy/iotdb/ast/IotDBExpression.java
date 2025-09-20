package com.fuzzy.iotdb.ast;

public interface IotDBExpression {

    default IotDBConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

    default void checkSyntax() {}

}
