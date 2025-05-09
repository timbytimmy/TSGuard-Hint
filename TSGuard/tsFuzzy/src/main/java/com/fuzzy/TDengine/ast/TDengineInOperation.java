package com.fuzzy.TDengine.ast;


import java.util.List;


public class TDengineInOperation implements TDengineExpression {

    private final TDengineExpression expr;
    private final List<TDengineExpression> listElements;
    private final boolean isTrue;

    public TDengineInOperation(TDengineExpression expr, List<TDengineExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public TDengineExpression getExpr() {
        return expr;
    }

    public List<TDengineExpression> getListElements() {
        return listElements;
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant leftVal = expr.getExpectedValue();
        if (leftVal.isNull()) return TDengineConstant.createNullConstant();

        for (TDengineExpression rightExpr : listElements) {
            TDengineConstant rightVal = rightExpr.getExpectedValue();
            TDengineConstant isEquals = leftVal.isEquals(rightVal);
            if (isEquals.isNull()) return TDengineConstant.createNullConstant();
            else if (isEquals.asBooleanNotNull()) return TDengineConstant.createBoolean(isTrue);
        }
        return TDengineConstant.createBoolean(!isTrue);
    }

    public boolean isTrue() {
        return isTrue;
    }
}
