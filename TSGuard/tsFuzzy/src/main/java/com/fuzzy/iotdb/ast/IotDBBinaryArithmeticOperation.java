package com.fuzzy.iotdb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.iotdb.IotDBSchema.IotDBDataType;
import com.fuzzy.iotdb.ast.IotDBCastOperation.CastType;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;

@Slf4j
public class IotDBBinaryArithmeticOperation implements IotDBExpression {

    private final IotDBExpression left;
    private final IotDBExpression right;
    private final IotDBBinaryArithmeticOperator op;

    public enum IotDBBinaryArithmeticOperator {
        PLUS("+") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
                // 算法精度扩大至15, 计算结果保留高精度, 比较运算低精度, 确保测试预言精度正确性
                return applyArithmeticOperation(left, right, (l, r) -> l.add(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        SUBTRACT("-") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.subtract(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        MULTIPLY("*") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) -> l.multiply(r)
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        DIVIDE("/") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
                return applyArithmeticOperation(left, right, (l, r) ->
                        l.divide(r, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }

        },
        MODULO("%") {
            @Override
            public IotDBConstant apply(IotDBConstant left, IotDBConstant right) {
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

        private static IotDBConstant applyArithmeticOperation(IotDBConstant left, IotDBConstant right,
                                                              BinaryOperator<BigDecimal> op) {
            if (left.isNull() || right.isNull()) {
                return IotDBConstant.createNullConstant();
            } else {
                BigDecimal leftVal = left.castAs(CastType.BIGDECIMAL).getBigDecimalValue();
                BigDecimal rightVal = right.castAs(CastType.BIGDECIMAL).getBigDecimalValue();

                try {
                    BigDecimal value = op.apply(leftVal, rightVal);
                    return IotDBConstant.createBigDecimalConstant(value);
                } catch (ArithmeticException e) {
                    log.warn("除数不能为0.");
                    throw new IgnoreMeException();
                }
            }
        }

        IotDBBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract IotDBConstant apply(IotDBConstant left, IotDBConstant right);

        public static IotDBBinaryArithmeticOperator getRandom() {
            return Randomly.fromOptions(IotDBBinaryArithmeticOperator.PLUS, IotDBBinaryArithmeticOperator.SUBTRACT,
                    IotDBBinaryArithmeticOperator.MULTIPLY);
        }

        public static IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator getRandomOperatorForTimestamp() {
            return Randomly.fromOptions(IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.PLUS,
                    IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.SUBTRACT);
        }
    }

    public IotDBBinaryArithmeticOperation(IotDBExpression left, IotDBExpression right, IotDBBinaryArithmeticOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
        // 数据类型要求：INT32、INT64、FLOAT、DOUBLE
        // 输出类型：DOUBLE
        // null -> null
        IotDBDataType leftDataType = left.getExpectedValue().getType();
        IotDBDataType rightDataType = right.getExpectedValue().getType();
        if (IotDBDataType.BOOLEAN.equals(leftDataType) || IotDBDataType.BOOLEAN.equals(rightDataType))
            throw new ReGenerateExpressionException("IotDBBinaryArithmeticOperation");
    }

    @Override
    public IotDBConstant getExpectedValue() {
        IotDBConstant leftExpected = left.getExpectedValue();
        IotDBConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    public IotDBExpression getLeft() {
        return left;
    }

    public IotDBBinaryArithmeticOperator getOp() {
        return op;
    }

    public IotDBExpression getRight() {
        return right;
    }

}
