package com.fuzzy.iotdb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;

public class IotDBBinaryLogicalOperation implements IotDBExpression {

    private final IotDBExpression left;
    private final IotDBExpression right;
    private final IotDBBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum IotDBBinaryLogicalOperator {
        AND("AND", "&", "&&") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
                if (left.isNull() || right.isNull()) return IotDBConstant.createNullConstant();
                else return IotDBConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
            }
        },
        OR("OR", "|", "||") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
                if (left.isNull() || right.isNull()) return IotDBConstant.createNullConstant();
                else return IotDBConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
            }
        };

        private final String[] textRepresentations;

        IotDBBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract IotDBConstant apply(IotDBConstant left, IotDBConstant right);

        public static IotDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static IotDBBinaryLogicalOperator getOperatorByText(String text) {
            for (IotDBBinaryLogicalOperator value : IotDBBinaryLogicalOperator.values()) {
                for (String textRepresentation : value.textRepresentations) {
                    if (textRepresentation.equalsIgnoreCase(text)) return value;
                }
            }
            throw new UnsupportedOperationException();
        }
    }

    public IotDBBinaryLogicalOperation(IotDBExpression left, IotDBExpression right, IotDBBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public IotDBExpression getLeft() {
        return left;
    }

    public IotDBBinaryLogicalOperator getOp() {
        return op;
    }

    public IotDBExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public void checkSyntax() {
        // 701: The output type of the expression in WHERE clause should be BOOLEAN, actual data type: INT32.
        if (!(this.left.getExpectedValue().isBoolean() && right.getExpectedValue().isBoolean())) {
            throw new ReGenerateExpressionException("IotDBBinaryLogicalOperation");
        }
    }

    @Override
    public IotDBConstant getExpectedValue() {
        IotDBConstant leftExpected = left.getExpectedValue();
        IotDBConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
