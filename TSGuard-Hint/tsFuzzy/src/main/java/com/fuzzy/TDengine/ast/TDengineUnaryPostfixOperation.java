package com.fuzzy.TDengine.ast;

import com.fuzzy.common.gen.ReGenerateExpressionException;

public class TDengineUnaryPostfixOperation implements TDengineExpression {

    private final TDengineExpression expression;
    private final UnaryPostfixOperator operator;
    private boolean negate;

    public enum UnaryPostfixOperator {
        IS_NULL
    }

    public TDengineUnaryPostfixOperation(TDengineExpression expr, UnaryPostfixOperator op, boolean negate) {
        this.expression = expr;
        this.operator = op;
        this.setNegate(negate);
    }

    @Override
    public void checkSyntax() {
        if (expression.getExpectedValue().isBoolean())
            throw new ReGenerateExpressionException("IS NULL前不支持BOOLEAN");
    }

    public TDengineExpression getExpression() {
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
    public TDengineConstant getExpectedValue() {
        boolean val;
        TDengineConstant expectedValue = expression.getExpectedValue();
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
        return TDengineConstant.createBoolean(val);
    }

}
