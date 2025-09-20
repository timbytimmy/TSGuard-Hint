package com.fuzzy.TDengine.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.TDengine.ast.TDengineCastOperation.CastType;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;

@Slf4j
public class TDengineBinaryArithmeticOperation implements TDengineExpression {

    private final TDengineExpression left;
    private final TDengineExpression right;
    private final TDengineBinaryArithmeticOperator op;

    public enum TDengineBinaryArithmeticOperator {
        PLUS("+") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.add(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        SUBTRACT("-") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.subtract(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        MULTIPLY("*") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.multiply(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        DIVIDE("/") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        l.divide(r, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
        MODULO("%") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                // 小数求模
                if (left.castAs(CastType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0
                        || right.castAs(CastType.BIGDECIMAL).getBigDecimalValue()
                        .remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) != 0) {
                    log.warn("不支持小数求模");
                    throw new IgnoreMeException();
                }

                return applyArithmeticOperation(left, right, (l, r) -> l.remainder(r));
            }
        };

        private String textRepresentation;

        private static TDengineConstant applyArithmeticOperation(TDengineConstant left, TDengineConstant right,
                                                                 BinaryOperator<BigDecimal> op) {
            if (left.isNull() || right.isNull()) {
                return TDengineConstant.createNullConstant();
            } else {
                BigDecimal leftVal = left.castAs(CastType.BIGDECIMAL).getBigDecimalValue();
                BigDecimal rightVal = right.castAs(CastType.BIGDECIMAL).getBigDecimalValue();

                try {
                    BigDecimal value = op.apply(leftVal, rightVal);
                    return TDengineConstant.createBigDecimalConstant(value);
                } catch (ArithmeticException e) {
                    log.warn("除数不能为0.");
                    throw new IgnoreMeException();
                }
            }
        }

        TDengineBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract TDengineConstant apply(TDengineConstant left, TDengineConstant right);

        public static TDengineBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static TDengineBinaryArithmeticOperator getRandomOperatorForTimestamp() {
            return Randomly.fromOptions(TDengineBinaryArithmeticOperator.PLUS,
                    TDengineBinaryArithmeticOperator.SUBTRACT);
        }
    }

    public TDengineBinaryArithmeticOperation(TDengineExpression left, TDengineExpression right, TDengineBinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
        if ((left instanceof TDengineColumnReference
                && ((TDengineColumnReference) left).getColumn().getName()
                .equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                && right instanceof TDengineColumnReference
                && ((TDengineColumnReference) right).getColumn().getName()
                .equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName()))) {
            log.warn("Invalid operation");
            throw new ReGenerateExpressionException("Between");
        }
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant leftExpected = left.getExpectedValue();
        TDengineConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    public TDengineExpression getLeft() {
        return left;
    }

    public TDengineBinaryArithmeticOperator getOp() {
        return op;
    }

    public TDengineExpression getRight() {
        return right;
    }

}
