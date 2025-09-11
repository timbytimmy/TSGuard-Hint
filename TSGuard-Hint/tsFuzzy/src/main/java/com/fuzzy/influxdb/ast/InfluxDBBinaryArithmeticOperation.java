package com.fuzzy.influxdb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.influxdb.ast.InfluxDBCastOperation.CastType;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;

@Slf4j
public class InfluxDBBinaryArithmeticOperation implements InfluxDBExpression {

    private final InfluxDBExpression left;
    private final InfluxDBExpression right;
    private final InfluxDBBinaryArithmeticOperator op;

    public enum InfluxDBBinaryArithmeticOperator {
        PLUS("+") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.add(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        SUBTRACT("-") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.subtract(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        MULTIPLY("*") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.multiply(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        DIVIDE("/") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        l.divide(r, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        MODULO("%") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                // 小数求模
                if (left.castAs(CastType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0
                        || right.castAs(CastType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                    throw new IgnoreMeException();
                }

                return applyArithmeticOperation(left, right, (l, r) -> l.remainder(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        };

        private String textRepresentation;

        private static InfluxDBConstant applyArithmeticOperation(InfluxDBConstant left, InfluxDBConstant right,
                                                                 BinaryOperator<BigDecimal> op) {
            if (left.isNull() || right.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (left.isBoolean() || right.isBoolean()) {
                log.warn("Boolean值不支持算数运算.");
                throw new IgnoreMeException();
            } else {
                BigDecimal leftVal = left.castAs(CastType.BIGDECIMAL).getBigDecimalValue();
                BigDecimal rightVal = right.castAs(CastType.BIGDECIMAL).getBigDecimalValue();

                try {
                    BigDecimal value = op.apply(leftVal, rightVal);
                    return InfluxDBConstant.createBigDecimalConstant(value);
                } catch (ArithmeticException e) {
                    log.warn("除数不能为0.");
                    throw new IgnoreMeException();
                }
            }
        }

        InfluxDBBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right);

        public static InfluxDBBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(PLUS, SUBTRACT, MULTIPLY);
        }

        public static InfluxDBBinaryArithmeticOperator getRandomOperatorForTimestamp() {
            return Randomly.fromOptions(PLUS, SUBTRACT);
        }
    }

    public InfluxDBBinaryArithmeticOperation(InfluxDBExpression left, InfluxDBExpression right, InfluxDBBinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
        if (!(left.getExpectedValue().isNumber() && right.getExpectedValue().isNumber()))
            throw new ReGenerateExpressionException("二元算术运算仅支持数值类型");
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        InfluxDBConstant leftExpected = left.getExpectedValue();
        InfluxDBConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    public InfluxDBExpression getLeft() {
        return left;
    }

    public InfluxDBBinaryArithmeticOperator getOp() {
        return op;
    }

    public InfluxDBExpression getRight() {
        return right;
    }
}
