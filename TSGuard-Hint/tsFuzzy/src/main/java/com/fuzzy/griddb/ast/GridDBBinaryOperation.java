package com.fuzzy.griddb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.griddb.GridDBSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BinaryOperator;

@Slf4j
public class GridDBBinaryOperation implements GridDBExpression {

    private final GridDBExpression left;
    private final GridDBExpression right;
    private final GridDBBinaryOperator op;

    public enum GridDBBinaryOperator {
        AND("&") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        };

        private String textRepresentation;

        private static GridDBConstant applyBitOperation(GridDBConstant left, GridDBConstant right,
                                                        BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return GridDBConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(GridDBSchema.GridDBDataType.LONG).getInt();
                long rightVal = right.castAs(GridDBSchema.GridDBDataType.LONG).getInt();
                long value = op.apply(leftVal, rightVal);
                return GridDBConstant.createInt64Constant(value);
            }
        }

        GridDBBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract GridDBConstant apply(GridDBConstant left, GridDBConstant right);

        public static GridDBBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public GridDBBinaryOperation(GridDBExpression left, GridDBExpression right, GridDBBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    private boolean isLong(GridDBConstant constant) {
        if (constant.isInt()) return true;
        else if (constant.isBigDecimal()) return constant.getBigDecimalValue().stripTrailingZeros().scale() <= 0;
        return false;
    }

    @Override
    public void checkSyntax() {
        if (!isLong(left.getExpectedValue()) || !isLong(right.getExpectedValue()))
            throw new ReGenerateExpressionException("按位运算仅支持整型");
    }

    @Override
    public GridDBConstant getExpectedValue() {
        GridDBConstant leftExpected = left.getExpectedValue();
        GridDBConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    public GridDBExpression getLeft() {
        return left;
    }

    public GridDBBinaryOperator getOp() {
        return op;
    }

    public GridDBExpression getRight() {
        return right;
    }

    @Override
    public boolean hasColumn() {
        return left.hasColumn() || right.hasColumn();
    }
}
