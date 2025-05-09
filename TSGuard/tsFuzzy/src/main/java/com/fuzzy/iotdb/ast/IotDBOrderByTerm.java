package com.fuzzy.iotdb.ast;


import com.fuzzy.Randomly;

public class IotDBOrderByTerm implements IotDBExpression {

    private final IotDBOrder order;
    private final IotDBExpression expr;

    public enum IotDBOrder {
        ASC, DESC;

        public static IotDBOrder getRandomOrder() {
            return Randomly.fromOptions(IotDBOrder.values());
        }
    }

    public IotDBOrderByTerm(IotDBExpression expr, IotDBOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public IotDBOrder getOrder() {
        return order;
    }

    public IotDBExpression getExpr() {
        return expr;
        // TODO
        /*return expr.getExpectedValue().isNull() ? new IotDBStringExpression("",
                IotDBConstant.createDoubleQuotesStringConstant("")) : expr;*/
    }

    @Override
    public IotDBConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
