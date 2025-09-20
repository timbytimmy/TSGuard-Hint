package com.fuzzy.griddb.ast;


import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GridDBBetweenOperation implements GridDBExpression {

    private final GridDBExpression expr;
    private final GridDBExpression left;
    private final GridDBExpression right;
    private final boolean isSymmetric;

    public GridDBBetweenOperation(GridDBExpression expr, GridDBExpression left, GridDBExpression right,
                                  boolean isSymmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        this.isSymmetric = isSymmetric;
    }

    public GridDBExpression getExpr() {
        return expr;
    }

    public GridDBExpression getLeft() {
        return left;
    }

    public GridDBExpression getRight() {
        return right;
    }

    @Override
    public void checkSyntax() {
        GridDBConstant exprExpectedValue = expr.getExpectedValue();
        GridDBConstant leftExpectedValue = left.getExpectedValue();
        GridDBConstant rightExpectedValue = right.getExpectedValue();
        if (!GridDBConstant.dataTypeIsEqual(exprExpectedValue, leftExpectedValue)
                || !GridDBConstant.dataTypeIsEqual(exprExpectedValue, rightExpectedValue)) {
            log.warn("dataType is not equal");
            throw new ReGenerateExpressionException("Between");
        }
    }

    @Override
    public GridDBConstant getExpectedValue() {
        GridDBBinaryComparisonOperation leftComparison = new GridDBBinaryComparisonOperation(left, expr,
                GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        GridDBBinaryComparisonOperation rightComparison = new GridDBBinaryComparisonOperation(expr, right,
                GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        GridDBBinaryLogicalOperation andOperation = new GridDBBinaryLogicalOperation(leftComparison,
                rightComparison, GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND);
        if (isSymmetric) {
            GridDBBinaryComparisonOperation leftComparison2 = new GridDBBinaryComparisonOperation(right, expr,
                    GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
            GridDBBinaryComparisonOperation rightComparison2 = new GridDBBinaryComparisonOperation(expr, left,
                    GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
            GridDBBinaryLogicalOperation andOperation2 = new GridDBBinaryLogicalOperation(leftComparison2,
                    rightComparison2, GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND);
            GridDBBinaryLogicalOperation orOp = new GridDBBinaryLogicalOperation(andOperation, andOperation2,
                    GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

    @Override
    public boolean hasColumn() {
        return expr.hasColumn() || left.hasColumn() || right.hasColumn();
    }

}
