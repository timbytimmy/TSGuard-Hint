package com.fuzzy.griddb.ast;


import com.fuzzy.Randomly;

public class GridDBOrderByTerm implements GridDBExpression {

    private final GridDBOrder order;
    private final GridDBExpression expr;

    public enum GridDBOrder {
        ASC, DESC;

        public static GridDBOrder getRandomOrder() {
            return Randomly.fromOptions(ASC);
        }
    }

    public GridDBOrderByTerm(GridDBExpression expr, GridDBOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public GridDBOrder getOrder() {
        return order;
    }

    public GridDBExpression getExpr() {
        return expr;
    }

    @Override
    public GridDBConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
