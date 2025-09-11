package com.fuzzy.griddb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.griddb.GridDBSchema;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;

@Slf4j
public class GridDBBinaryArithmeticOperation implements GridDBExpression {

    private final GridDBExpression left;
    private final GridDBExpression right;
    private final GridDBBinaryArithmeticOperator op;

    public enum GridDBBinaryArithmeticOperator {
        PLUS("+") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.add(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        SUBTRACT("-") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.subtract(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        MULTIPLY("*") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.multiply(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        DIVIDE("/") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        l.divide(r, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        MODULO("%") {
            @Override
            public GridDBConstant apply(GridDBConstant left, GridDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.remainder(r));
            }
        };

        private String textRepresentation;

        private static GridDBConstant applyArithmeticOperation(GridDBConstant left, GridDBConstant right,
                                                               BinaryOperator<BigDecimal> op) {
            if (left.isNull() || right.isNull()) {
                return GridDBConstant.createNullConstant();
            } else {
                BigDecimal leftVal = left.castAs(GridDBSchema.GridDBDataType.BIGDECIMAL).getBigDecimalValue();
                BigDecimal rightVal = right.castAs(GridDBSchema.GridDBDataType.BIGDECIMAL).getBigDecimalValue();

                try {
                    BigDecimal value = op.apply(leftVal, rightVal);
                    if (checkPrecision(value)) {
                        log.warn("整数长度超过15位");
                        throw new IgnoreMeException();
                    }
                    return GridDBConstant.createBigDecimalConstant(value);
                } catch (ArithmeticException e) {
                    log.warn("除数不能为0.");
                    throw new IgnoreMeException();
                }
            }
        }

        public static boolean checkPrecision(BigDecimal number) {
            String plainString = number.toPlainString();
            String[] strNumber = plainString.replace("-", "").split("\\.");
            return strNumber[0].length() > DoubleArithmeticPrecisionConstant.scale;
        }

        GridDBBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract GridDBConstant apply(GridDBConstant left, GridDBConstant right);

        public static GridDBBinaryArithmeticOperator getRandom() {
//            return Randomly.fromOptions(values());
            return Randomly.fromOptions(PLUS, SUBTRACT, MULTIPLY, MODULO);
        }

        public static GridDBBinaryArithmeticOperator getRandomOperatorForTimestamp() {
            return Randomly.fromOptions(GridDBBinaryArithmeticOperator.PLUS,
                    GridDBBinaryArithmeticOperator.SUBTRACT);
        }

    }

    public GridDBBinaryArithmeticOperation(GridDBExpression left, GridDBExpression right, GridDBBinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
        if (!(left.getExpectedValue().isNumber() && right.getExpectedValue().isNumber())) {
            log.warn("dataType is not equal");
            throw new ReGenerateExpressionException("Arithmetic");
        }

        // 小数求模 -> 不支持
        if (op == GridDBBinaryArithmeticOperator.MODULO
                && (left.getExpectedValue().isDouble() || right.getExpectedValue().isDouble())) {
            throw new ReGenerateExpressionException("Arithmetic");
        }
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

    public GridDBBinaryArithmeticOperator getOp() {
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
