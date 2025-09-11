package com.fuzzy.TDengine.ast;


import com.fuzzy.Randomly;

public class TDengineOrderByTerm implements TDengineExpression {

    private final TDengineOrder order;
    private final TDengineExpression expr;

    public enum TDengineOrder {
        ASC, DESC;

        public static TDengineOrder getRandomOrder() {
            return Randomly.fromOptions(ASC);
        }
    }

    public TDengineOrderByTerm(TDengineExpression expr, TDengineOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public TDengineOrder getOrder() {
        return order;
    }

    public TDengineExpression getExpr() {
        return expr;
    }

    @Override
    public TDengineConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
