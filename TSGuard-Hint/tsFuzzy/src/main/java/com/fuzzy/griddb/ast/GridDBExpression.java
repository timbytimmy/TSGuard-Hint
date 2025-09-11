package com.fuzzy.griddb.ast;

public interface GridDBExpression {

    default GridDBConstant getExpectedValue() {
        throw new AssertionError("Not supported for this operator");
    }

    default void checkSyntax() {}

    default boolean hasColumn() {
        return false;
    }

}
