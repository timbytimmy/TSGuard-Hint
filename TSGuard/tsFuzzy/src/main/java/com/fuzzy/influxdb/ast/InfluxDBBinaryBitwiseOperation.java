package com.fuzzy.influxdb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.influxdb.ast.InfluxDBCastOperation.CastType;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BinaryOperator;

@Slf4j
public class InfluxDBBinaryBitwiseOperation implements InfluxDBExpression {

    private final InfluxDBExpression left;
    private final InfluxDBExpression right;
    private final InfluxDBBinaryBitwiseOperator op;

    public enum InfluxDBBinaryBitwiseOperator {
        AND("&") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                if (left.isBoolean() || right.isBoolean())
                    return applyBitOperationForBoolean(left, right, InfluxDBBinaryBitwiseOperator.AND);

                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                if (left.isBoolean() || right.isBoolean())
                    return applyBitOperationForBoolean(left, right, InfluxDBBinaryBitwiseOperator.OR);

                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        },
        XOR("^") {
            @Override
            public InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right) {
                if (left.isBoolean() || right.isBoolean())
                    return applyBitOperationForBoolean(left, right, InfluxDBBinaryBitwiseOperator.XOR);

                return applyBitOperation(left, right, (l, r) -> l ^ r);
            }
        };

        private String textRepresentation;

        private static InfluxDBConstant applyBitOperationForBoolean(InfluxDBConstant left, InfluxDBConstant right,
                                                                    InfluxDBBinaryBitwiseOperator op) {
            if (InfluxDBBinaryBitwiseOperator.AND.equals(op)) {
                // Boolean值间取 &&
                // Boolean值与Int 直接返回false
                if (left.isBoolean() && right.isBoolean())
                    return InfluxDBConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
                else if (left.isBoolean() && right.isInt() || left.isInt() && right.isBoolean())
                    return InfluxDBConstant.createBoolean(false);

            } else if (InfluxDBBinaryBitwiseOperator.OR.equals(op)) {
                // Boolean值间取 ||
                // Boolean值与UInt, 取Boolean值
                // Boolean值与Int, FALSE
                if (left.isBoolean() && right.isBoolean())
                    return InfluxDBConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
                else if (left.isBoolean() && right.isUInt())
                    return left;
                else if (left.isUInt() && right.isBoolean())
                    return right;
                else return InfluxDBConstant.createBoolean(false);

            } else if (InfluxDBBinaryBitwiseOperator.XOR.equals(op)) {
                // Boolean值间取 异或
                // Boolean值与Int, 取Boolean
                if (left.isBoolean() && right.isBoolean())
                    return InfluxDBConstant.createBoolean(left.asBooleanNotNull() != right.asBooleanNotNull());
                else if (left.isBoolean() && right.isInt())
                    return InfluxDBConstant.createBoolean(false);
                else if (left.isInt() && right.isBoolean())
                    return InfluxDBConstant.createBoolean(false);

            } else {
                throw new AssertionError(String.format("applyBitOperationForBoolean 不支持操作类型:%s", op.toString()));
            }

            // Boolean与其他值
            throw new IgnoreMeException();
        }

        private static InfluxDBConstant applyBitOperation(InfluxDBConstant left, InfluxDBConstant right,
                                                          BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(CastType.SIGNED).getInt();
                long rightVal = right.castAs(CastType.SIGNED).getInt();
                long value = op.apply(leftVal, rightVal);
                return InfluxDBConstant.createNoSuffixIntConstant(value);
            }
        }

        InfluxDBBinaryBitwiseOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract InfluxDBConstant apply(InfluxDBConstant left, InfluxDBConstant right);

        public static InfluxDBBinaryBitwiseOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public InfluxDBBinaryBitwiseOperation(InfluxDBExpression left, InfluxDBExpression right, InfluxDBBinaryBitwiseOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
        if (left.getExpectedValue().isDouble() || right.getExpectedValue().isDouble())
            throw new ReGenerateExpressionException("二元位运算不支持浮点类型");
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

    public InfluxDBBinaryBitwiseOperator getOp() {
        return op;
    }

    public InfluxDBExpression getRight() {
        return right;
    }
}
