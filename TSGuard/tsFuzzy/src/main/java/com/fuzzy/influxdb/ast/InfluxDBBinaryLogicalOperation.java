package com.fuzzy.influxdb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;

public class InfluxDBBinaryLogicalOperation implements InfluxDBExpression {

    private final InfluxDBExpression left;
    private final InfluxDBExpression right;
    private final InfluxDBBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum InfluxDBBinaryLogicalOperator {
        AND("AND", "and") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                if (left.isNull() || right.isNull()) return InfluxDBConstant.createNullConstant();
                else return InfluxDBConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
            }
        },
        OR("OR", "or") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                if (left.isNull() || left.isNull()) return InfluxDBConstant.createBoolean(true);
                else return InfluxDBConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
            }
        };

        private final String[] textRepresentations;

        InfluxDBBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right);

        public static InfluxDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static InfluxDBBinaryLogicalOperator getOperatorByText(String text) {
            for (InfluxDBBinaryLogicalOperator value : InfluxDBBinaryLogicalOperator.values()) {
                for (String textRepresentation : value.textRepresentations) {
                    if (textRepresentation.equalsIgnoreCase(text)) return value;
                }
            }
            throw new UnsupportedOperationException();
        }
    }

    public InfluxDBBinaryLogicalOperation(InfluxDBExpression left, InfluxDBExpression right, InfluxDBBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    @Override
    public void checkSyntax() {
        if (!(left.getExpectedValue().isBoolean() && right.getExpectedValue().isBoolean()))
            throw new ReGenerateExpressionException("二元逻辑操作仅支持BOOLEAN类型");
    }

    public InfluxDBExpression getLeft() {
        return left;
    }

    public InfluxDBBinaryLogicalOperator getOp() {
        return op;
    }

    public InfluxDBExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        InfluxDBConstant leftExpected = left.getExpectedValue();
        InfluxDBConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
