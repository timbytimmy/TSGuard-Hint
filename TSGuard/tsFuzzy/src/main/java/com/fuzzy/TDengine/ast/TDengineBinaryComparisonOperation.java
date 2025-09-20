package com.fuzzy.TDengine.ast;


import com.fuzzy.Randomly;

public class TDengineBinaryComparisonOperation implements TDengineExpression {

    public enum BinaryComparisonOperator {
        EQUALS("==") {
            @Override
            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        NOT_EQUALS("!=", "<>") {
            @Override
            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
                TDengineConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isNull()) return TDengineConstant.createNullConstant();
                return TDengineConstant.createBoolean(!isEquals.asBooleanNotNull());
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        LESS("<") {
            @Override
            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER;
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
                TDengineConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isNull()) return TDengineConstant.createNullConstant();
                if (!lessThan.asBooleanNotNull()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER_EQUALS;
            }
        },
        GREATER(">") {
            @Override
            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
                TDengineConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return TDengineConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return TDengineConstant.createFalse();
                } else {
                    TDengineConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return TDengineConstant.createNullConstant();
                    return TDengineConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS;
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
                TDengineConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return TDengineConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return TDengineConstant.createTrue();
                } else {
                    TDengineConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return TDengineConstant.createNullConstant();
                    return TDengineConstant.createBoolean(!applyLess.asBooleanNotNull());
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
//            public TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal) {
//                if (leftVal.isNull() || rightVal.isNull()) {
//                    return TDengineConstant.createNullConstant();
//                }
//                String leftStr = leftVal.castAsString();
//                String rightStr = rightVal.castAsString();
//                boolean matches = LikeImplementationHelper.match(leftStr, rightStr, 0, 0, false);
//                return TDengineConstant.createBoolean(matches);
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

        public abstract TDengineConstant getExpectedValue(TDengineConstant leftVal, TDengineConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }

        public abstract BinaryComparisonOperator reverseInequality();
    }

    private final TDengineExpression left;
    private final TDengineExpression right;
    private final BinaryComparisonOperator op;

    public TDengineBinaryComparisonOperation(TDengineExpression left, TDengineExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public TDengineExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public TDengineExpression getRight() {
        return right;
    }

    @Override
    public TDengineConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

    public boolean containsEqual() {
        return BinaryComparisonOperator.EQUALS.equals(this.op)
                || BinaryComparisonOperator.GREATER_EQUALS.equals(this.op)
                || BinaryComparisonOperator.LESS_EQUALS.equals(this.op);
    }
}
