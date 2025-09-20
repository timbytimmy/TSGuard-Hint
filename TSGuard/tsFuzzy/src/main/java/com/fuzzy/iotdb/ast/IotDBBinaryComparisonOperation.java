package com.fuzzy.iotdb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.iotdb.IotDBSchema.IotDBDataType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IotDBBinaryComparisonOperation implements IotDBExpression {

    public enum BinaryComparisonOperator {
        EQUALS("==") {
            @Override
            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        NOT_EQUALS("!=", "<>") {
            @Override
            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
                IotDBConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isNull()) return IotDBConstant.createNullConstant();
                else return IotDBConstant.createBoolean(!isEquals.asBooleanNotNull());
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        LESS("<") {
            @Override
            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER;
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
                IotDBConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isNull()) return IotDBConstant.createNullConstant();
                else if (!lessThan.asBooleanNotNull()) return leftVal.isEquals(rightVal);
                else return lessThan;
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER_EQUALS;
            }
        },
        GREATER(">") {
            @Override
            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
                IotDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return IotDBConstant.createNullConstant();
                else if (equals.asBooleanNotNull()) return IotDBConstant.createFalse();
                else {
                    IotDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return IotDBConstant.createNullConstant();
                    return IotDBConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS;
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
                IotDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return IotDBConstant.createNullConstant();
                else if (equals.asBooleanNotNull()) return IotDBConstant.createTrue();
                else {
                    IotDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return IotDBConstant.createNullConstant();
                    return IotDBConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS_EQUALS;
            }
        },
//        LIKE("LIKE") {
//
//            @Override
//            public IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal) {
//                if (leftVal.isNull() || rightVal.isNull()) {
//                    return IotDBConstant.createNullConstant();
//                }
//                String leftStr = leftVal.castAsString();
//                String rightStr = rightVal.castAsString();
//                boolean matches = LikeImplementationHelper.match(leftStr, rightStr, 0, 0, false);
//                return IotDBConstant.createBoolean(matches);
//            }
//
//        }
        ;

        private final String[] textRepresentations;

        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        BinaryComparisonOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract IotDBConstant getExpectedValue(IotDBConstant leftVal, IotDBConstant rightVal);

        public abstract BinaryComparisonOperator reverseInequality();

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }
    }

    private final IotDBExpression left;
    private final IotDBExpression right;
    private final BinaryComparisonOperator op;

    public IotDBBinaryComparisonOperation(IotDBExpression left, IotDBExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public IotDBExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public IotDBExpression getRight() {
        return right;
    }

    @Override
    public void checkSyntax() {
        if (!IotDBConstant.dataTypeIsEqual(left.getExpectedValue(), right.getExpectedValue())) {
            log.warn("data type is not equal");
            throw new ReGenerateExpressionException("IotDBBinaryComparisonOperation");
        } else if (IotDBDataType.BOOLEAN.isEquals(left.getExpectedValue().getType())
                && op != BinaryComparisonOperator.EQUALS
                && op != BinaryComparisonOperator.NOT_EQUALS) {
            log.warn("Boolean值不支持不等比较");
            throw new ReGenerateExpressionException("IotDBBinaryComparisonOperation");
        }
    }

    @Override
    public IotDBConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

}
