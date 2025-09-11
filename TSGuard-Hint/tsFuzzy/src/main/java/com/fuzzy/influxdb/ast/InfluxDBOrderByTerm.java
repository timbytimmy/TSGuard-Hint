package com.fuzzy.influxdb.ast;


import com.fuzzy.Randomly;

public class InfluxDBOrderByTerm implements InfluxDBExpression {

    private final InfluxDBOrder order;
    private final InfluxDBExpression expr;

    public enum InfluxDBOrder {
        ASC, DESC;

        public static InfluxDBOrder getRandomOrder() {
            return Randomly.fromOptions(InfluxDBOrder.values());
        }
    }

    public InfluxDBOrderByTerm(InfluxDBExpression expr, InfluxDBOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public InfluxDBOrder getOrder() {
        return order;
    }

    public InfluxDBExpression getExpr() {
        return expr.getExpectedValue().isNull() ? new InfluxDBStringExpression("",
                InfluxDBConstant.createDoubleQuotesStringConstant("")) : expr;
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        throw new AssertionError(this);
    }

}
