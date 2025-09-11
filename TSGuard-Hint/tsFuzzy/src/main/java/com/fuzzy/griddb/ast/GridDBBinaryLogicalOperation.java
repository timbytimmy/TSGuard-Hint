package com.fuzzy.griddb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GridDBBinaryLogicalOperation implements GridDBExpression {

    private final GridDBExpression left;
    private final GridDBExpression right;
    private final GridDBBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum GridDBBinaryLogicalOperator {
        AND("AND") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                if (left.isNull() || right.isNull()) return GridDBConstant.createNullConstant();
                else return GridDBConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
            }
        },
        OR("OR") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                if (left.isNull() && right.isNull()) return GridDBConstant.createNullConstant();
                else if (left.isNull()) return GridDBConstant.createBoolean(right.asBooleanNotNull());
                else if (right.isNull()) return GridDBConstant.createBoolean(left.asBooleanNotNull());
                else return GridDBConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
            }
        };

        private final String[] textRepresentations;

        GridDBBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract GridDBConstant apply(GridDBConstant left, GridDBConstant right);

        public static GridDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static GridDBBinaryLogicalOperator getOperatorByText(String text) {
            for (GridDBBinaryLogicalOperator value : GridDBBinaryLogicalOperator.values()) {
                for (String textRepresentation : value.textRepresentations) {
                    if (textRepresentation.equalsIgnoreCase(text)) return value;
                }
            }
            throw new UnsupportedOperationException();
        }
    }

    public GridDBBinaryLogicalOperation(GridDBExpression left, GridDBExpression right, GridDBBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    @Override
    public void checkSyntax() {
        if (!(this.left.getExpectedValue().isBoolean() && right.getExpectedValue().isBoolean())) {
            log.warn("dataType is not BOOL");
            throw new ReGenerateExpressionException("GridDBBinaryLogicalOperation");
        }
    }

    public GridDBExpression getLeft() {
        return left;
    }

    public GridDBBinaryLogicalOperator getOp() {
        return op;
    }

    public GridDBExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public GridDBConstant getExpectedValue() {
        GridDBConstant leftExpected = left.getExpectedValue();
        GridDBConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

    @Override
    public boolean hasColumn() {
        return left.hasColumn() || right.hasColumn();
    }
}
