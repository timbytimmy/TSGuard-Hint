package com.fuzzy.influxdb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.influxdb.InfluxDBSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class InfluxDBBinaryComparisonOperation implements InfluxDBExpression {

    public enum BinaryComparisonOperator {
        EQUALS("=") {
            @Override
            public InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        NOT_EQUALS("!=", "<>") {
            @Override
            public InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal) {
                InfluxDBConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isNull()) return InfluxDBConstant.createNullConstant();
                return InfluxDBConstant.createBoolean(!isEquals.asBooleanNotNull());
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        LESS("<") {
            @Override
            public InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal) {
                if (leftVal.isBoolean() || rightVal.isBoolean()
                        || leftVal.isString() || rightVal.isString()) return InfluxDBConstant.createBoolean(false);

                return leftVal.isLessThan(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER;
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal) {
                if (leftVal.isBoolean() || rightVal.isBoolean()
                        || leftVal.isString() || rightVal.isString()) return InfluxDBConstant.createBoolean(false);

                InfluxDBConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isNull()) return InfluxDBConstant.createNullConstant();
                if (!lessThan.asBooleanNotNull()) return leftVal.isEquals(rightVal);
                else return lessThan;
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER_EQUALS;
            }
        },
        GREATER(">") {
            @Override
            public InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal) {
                if (leftVal.isBoolean() || rightVal.isBoolean()
                        || leftVal.isString() || rightVal.isString()) return InfluxDBConstant.createBoolean(false);

                InfluxDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return InfluxDBConstant.createNullConstant();
                if (equals.asBooleanNotNull()) return InfluxDBConstant.createBoolean(false);
                else {
                    InfluxDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return InfluxDBConstant.createNullConstant();
                    return InfluxDBConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS;
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal) {
                // 比较运算符涉及到Boolean值、String均为FALSE
                if (leftVal.isBoolean() || rightVal.isBoolean()
                        || leftVal.isString() || rightVal.isString()) return InfluxDBConstant.createBoolean(false);

                InfluxDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return InfluxDBConstant.createNullConstant();
                if (equals.asBooleanNotNull()) return InfluxDBConstant.createBoolean(true);
                else {
                    InfluxDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return InfluxDBConstant.createNullConstant();
                    return InfluxDBConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS_EQUALS;
            }

        };

        private final String[] textRepresentation;

        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentation);
        }

        BinaryComparisonOperator(String... textRepresentation) {
            this.textRepresentation = textRepresentation.clone();
        }

        public abstract InfluxDBConstant getExpectedValue(InfluxDBConstant leftVal, InfluxDBConstant rightVal);

        public abstract BinaryComparisonOperator reverseInequality();

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }

        public static BinaryComparisonOperator getRandomForTimestamp() {
            return Randomly.fromList(Arrays.stream(BinaryComparisonOperator.values())
                    .filter(op -> op != NOT_EQUALS).collect(Collectors.toList()));
        }
    }

    private final InfluxDBExpression left;
    private final InfluxDBExpression right;
    private final BinaryComparisonOperator op;

    public InfluxDBBinaryComparisonOperation(InfluxDBExpression left, InfluxDBExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public InfluxDBExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public InfluxDBExpression getRight() {
        return right;
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        // 类型不一致一律返回FALSE
        InfluxDBConstant leftExpectedValue = left.getExpectedValue();
        InfluxDBConstant rightExpectedValue = right.getExpectedValue();
        if (!leftExpectedValue.dataTypeIsEqual(rightExpectedValue)) return InfluxDBConstant.createBoolean(false);

        return op.getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    @Override
    public void checkSyntax() {
        // influxdb不支持 列 与 Uint进行比较，仅支持 列与Float比较
        if ((left instanceof InfluxDBColumnReference && right.getExpectedValue().isUInt())
                || (right instanceof InfluxDBColumnReference && left.getExpectedValue().isUInt()))
            throw new ReGenerateExpressionException("不支持列和UInt比较");
        else if (!left.getExpectedValue().dataTypeIsEqual(right.getExpectedValue())) {
            log.warn("data type is not equal");
            throw new ReGenerateExpressionException("IotDBBinaryComparisonOperation");
        } else if (InfluxDBSchema.InfluxDBDataType.BOOLEAN.equals(left.getExpectedValue().getType())
                && op != InfluxDBBinaryComparisonOperation.BinaryComparisonOperator.EQUALS
                && op != InfluxDBBinaryComparisonOperation.BinaryComparisonOperator.NOT_EQUALS) {
            log.warn("Boolean值不支持不等比较");
            throw new ReGenerateExpressionException("InfluxDBBinaryComparisonOperation");
        }
    }
}
