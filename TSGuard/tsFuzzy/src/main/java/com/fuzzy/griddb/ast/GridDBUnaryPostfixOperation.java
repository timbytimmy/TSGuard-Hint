package com.fuzzy.griddb.ast;

public class GridDBUnaryPostfixOperation implements GridDBExpression {

    private final GridDBExpression expression;
    private final UnaryPostfixOperator operator;
    private boolean negate;

    public enum UnaryPostfixOperator {
        IS_NULL
    }

    public GridDBUnaryPostfixOperation(GridDBExpression expr, UnaryPostfixOperator op, boolean negate) {
        this.expression = expr;
        this.operator = op;
        this.setNegate(negate);
    }

    public GridDBExpression getExpression() {
        return expression;
    }

    public UnaryPostfixOperator getOperator() {
        return operator;
    }

    public boolean isNegated() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    @Override
    public GridDBConstant getExpectedValue() {
        boolean val;
        GridDBConstant expectedValue = expression.getExpectedValue();
        switch (operator) {
            case IS_NULL:
                val = expectedValue.isNull();
                break;
            default:
                throw new AssertionError(operator);
        }
        if (negate) {
            val = !val;
        }
        return GridDBConstant.createBoolean(val);
    }

    @Override
    public boolean hasColumn() {
        return expression.hasColumn();
    }
}
