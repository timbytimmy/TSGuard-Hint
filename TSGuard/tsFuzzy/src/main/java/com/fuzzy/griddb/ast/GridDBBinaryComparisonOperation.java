package com.fuzzy.griddb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GridDBBinaryComparisonOperation implements GridDBExpression {

    public enum BinaryComparisonOperator {
        EQUALS("==") {
            @Override
            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        NOT_EQUALS("!=", "<>") {
            @Override
            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
                GridDBConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isNull()) return GridDBConstant.createNullConstant();
                return GridDBConstant.createBoolean(!isEquals.asBooleanNotNull());
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return this;
            }
        },
        LESS("<") {
            @Override
            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return GREATER;
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
                GridDBConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isNull()) return GridDBConstant.createNullConstant();
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
            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
                GridDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return GridDBConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return GridDBConstant.createBoolean(false);
                } else {
                    GridDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return GridDBConstant.createNullConstant();
                    return GridDBConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS;
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
                GridDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.isNull()) return GridDBConstant.createNullConstant();
                if (equals.asBooleanNotNull()) {
                    return GridDBConstant.createBoolean(true);
                } else {
                    GridDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) return GridDBConstant.createNullConstant();
                    return GridDBConstant.createBoolean(!applyLess.asBooleanNotNull());
                }
            }

            @Override
            public BinaryComparisonOperator reverseInequality() {
                return LESS_EQUALS;
            }
        },
        // IS / IS NOT
//        LIKE("LIKE") {
//
//            @Override
//            public GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal) {
//                if (leftVal.isNull() || rightVal.isNull()) {
//                    return GridDBConstant.createNullConstant();
//                }
//                String leftStr = leftVal.castAsString();
//                String rightStr = rightVal.castAsString();
//                boolean matches = LikeImplementationHelper.match(leftStr, rightStr, 0, 0, false);
//                return GridDBConstant.createBoolean(matches);
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

        public abstract GridDBConstant getExpectedValue(GridDBConstant leftVal, GridDBConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }

        public abstract BinaryComparisonOperator reverseInequality();
    }

    private final GridDBExpression left;
    private final GridDBExpression right;
    private final BinaryComparisonOperator op;

    public GridDBBinaryComparisonOperation(GridDBExpression left, GridDBExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public GridDBExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public GridDBExpression getRight() {
        return right;
    }

    @Override
    public GridDBConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

    public boolean containsEqual() {
        return BinaryComparisonOperator.EQUALS.equals(this.op)
                || BinaryComparisonOperator.GREATER_EQUALS.equals(this.op)
                || BinaryComparisonOperator.LESS_EQUALS.equals(this.op);
    }

    @Override
    public void checkSyntax() {
        if (!GridDBConstant.dataTypeIsEqual(left.getExpectedValue(), right.getExpectedValue())) {
            log.warn("dataType is not equal");
            throw new ReGenerateExpressionException("Comparison");
        }
    }

    @Override
    public boolean hasColumn() {
        return left.hasColumn() || right.hasColumn();
    }
}
