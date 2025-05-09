package com.fuzzy.iotdb.ast;

public class IotDBUnaryPostfixOperation implements IotDBExpression {

    private final IotDBExpression expression;
    private final UnaryPostfixOperator operator;
    private boolean negate;

    public enum UnaryPostfixOperator {
        IS_NULL
    }

    public IotDBUnaryPostfixOperation(IotDBExpression expr, UnaryPostfixOperator op, boolean negate) {
        this.expression = expr;
        this.operator = op;
        this.setNegate(negate);
    }

    public IotDBExpression getExpression() {
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
    public IotDBConstant getExpectedValue() {
        boolean val;
        IotDBConstant expectedValue = expression.getExpectedValue();
        switch (operator) {
            case IS_NULL:
                val = expectedValue.isNull();
                break;
            default:
                throw new AssertionError(operator);
        }
        if (negate) val = !val;
        return IotDBConstant.createBoolean(val);
    }

}
