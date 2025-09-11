package com.fuzzy.griddb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.griddb.GridDBSchema.GridDBDataType;
import com.fuzzy.griddb.ast.GridDBUnaryPrefixOperation.GridDBUnaryPrefixOperator;

public class GridDBUnaryPrefixOperation extends UnaryOperatorNode<GridDBExpression, GridDBUnaryPrefixOperator>
        implements GridDBExpression {

    public enum GridDBUnaryPrefixOperator implements Operator {
        PLUS("+") {
            @Override
            public GridDBConstant applyNotNull(GridDBConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public GridDBConstant applyNotNull(GridDBConstant expr) {
                if (expr.isString() || expr.isBoolean()) throw new IgnoreMeException();
                else if (expr.isBigDecimal()) return GridDBConstant.createBigDecimalConstant(
                        expr.getBigDecimalValue().negate());
                else if (expr.isInt()) {
                    if (GridDBDataType.INTEGER.equals(expr.getType()))
                        return GridDBConstant.createInt32Constant(-expr.getInt());
                    else if (GridDBDataType.LONG.equals(expr.getType()))
                        return GridDBConstant.createInt64Constant(-expr.getInt());
                    else throw new AssertionError(expr);
                } else if (expr.isDouble()) return GridDBConstant.createDoubleConstant(-expr.getDouble());
                else throw new AssertionError(expr);

            }
        };

        private String[] textRepresentations;

        GridDBUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract GridDBConstant applyNotNull(GridDBConstant expr);

        public static GridDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public GridDBUnaryPrefixOperation(GridDBExpression expr, GridDBUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public GridDBConstant getExpectedValue() {
        GridDBConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return GridDBConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    @Override
    public boolean hasColumn() {
        return expr.hasColumn();
    }
}
