package com.fuzzy.griddb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.griddb.ast.GridDBUnaryNotPrefixOperation.GridDBUnaryNotPrefixOperator;

public class GridDBUnaryNotPrefixOperation extends UnaryOperatorNode<GridDBExpression, GridDBUnaryNotPrefixOperator>
        implements GridDBExpression {

    public enum GridDBUnaryNotPrefixOperator implements Operator {
        NOT("NOT") {
            @Override
            public GridDBConstant applyNotNull(GridDBConstant expr) {
                return GridDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_DOUBLE(" = ") {
            @Override
            public GridDBConstant applyNotNull(GridDBConstant expr) {
                return GridDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_INT(" = ") {
            @Override
            public GridDBConstant applyNotNull(GridDBConstant expr) {
                return GridDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        };

        private String[] textRepresentations;

        GridDBUnaryNotPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract GridDBConstant applyNotNull(GridDBConstant expr);

        public static GridDBUnaryNotPrefixOperator getRandom(GridDBExpression subExpr) {
            GridDBExpression exprType = getNotPrefixTypeExpression(subExpr);

            GridDBUnaryNotPrefixOperator operator;
            if (exprType instanceof GridDBConstant && ((GridDBConstant) exprType).isBoolean())
                operator = GridDBUnaryNotPrefixOperator.NOT;
            else if (exprType instanceof GridDBConstant && ((GridDBConstant) exprType).isDouble())
                operator = GridDBUnaryNotPrefixOperator.NOT_DOUBLE;
            else operator = GridDBUnaryNotPrefixOperator.NOT_INT;
            return operator;
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public GridDBUnaryNotPrefixOperation(GridDBExpression expr, GridDBUnaryNotPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public GridDBConstant getExpectedValue() {
        GridDBConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return GridDBConstant.createBoolean(true);
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    public static GridDBUnaryNotPrefixOperation getNotUnaryPrefixOperation(GridDBExpression expr) {
        GridDBExpression exprType = getNotPrefixTypeExpression(expr);

        if (exprType instanceof GridDBConstant && ((GridDBConstant) exprType).isBoolean())
            return new GridDBUnaryNotPrefixOperation(expr, GridDBUnaryNotPrefixOperator.NOT);
        else if (exprType instanceof GridDBConstant && ((GridDBConstant) exprType).isDouble())
            return new GridDBUnaryNotPrefixOperation(expr, GridDBUnaryNotPrefixOperator.NOT_DOUBLE);
        else
            return new GridDBUnaryNotPrefixOperation(expr, GridDBUnaryNotPrefixOperator.NOT_INT);
    }

    private static GridDBExpression getNotPrefixTypeExpression(GridDBExpression expression) {
        GridDBExpression exprType;
        // 一元操作符Not类型 / 可计算函数 取决于表达式结果，列引用取决于其值类型
        // 二元位操作取决于返回值, cast取决于强转后类型, 二元逻辑、二元比较操作符、常量取决于返回值
        if (expression instanceof GridDBUnaryNotPrefixOperation || expression instanceof GridDBComputableFunction)
            exprType = expression.getExpectedValue();
        else if (expression instanceof GridDBUnaryPrefixOperation)
            exprType = ((GridDBUnaryPrefixOperation) expression).getExpression().getExpectedValue();
        else if (expression instanceof GridDBColumnReference)
            exprType = ((GridDBColumnReference) expression).getValue();
        else exprType = expression.getExpectedValue();
        return exprType;
    }

    @Override
    public boolean hasColumn() {
        return expr.hasColumn();
    }
}
