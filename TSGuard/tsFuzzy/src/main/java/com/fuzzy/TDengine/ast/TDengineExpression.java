package com.fuzzy.TDengine.ast;

public interface TDengineExpression {

    default TDengineConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

    default void checkSyntax() {}

}
