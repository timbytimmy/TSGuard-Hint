package com.fuzzy.TDengine.ast;


import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDengineBetweenOperation implements TDengineExpression {

    private final TDengineExpression expr;
    private final TDengineExpression left;
    private final TDengineExpression right;
    private final boolean isSymmetric;

    public TDengineBetweenOperation(TDengineExpression expr, TDengineExpression left, TDengineExpression right,
                                    boolean isSymmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        this.isSymmetric = isSymmetric;
    }

    public TDengineExpression getExpr() {
        return expr;
    }

    public TDengineExpression getLeft() {
        return left;
    }

    public TDengineExpression getRight() {
        return right;
    }

    @Override
    public void checkSyntax() {
        TDengineConstant exprExpectedValue = expr.getExpectedValue();
        TDengineConstant leftExpectedValue = left.getExpectedValue();
        TDengineConstant rightExpectedValue = right.getExpectedValue();
        if (!(exprExpectedValue.isString() && leftExpectedValue.isString() && rightExpectedValue.isString()
                || exprExpectedValue.isNumber() && leftExpectedValue.isNumber() && rightExpectedValue.isNumber())) {
            log.warn("The Type of three subExpression should be all Numeric or Text");
            throw new ReGenerateExpressionException("Between");
        }
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineBinaryComparisonOperation leftComparison = new TDengineBinaryComparisonOperation(left, expr,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        TDengineBinaryComparisonOperation rightComparison = new TDengineBinaryComparisonOperation(expr, right,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        TDengineBinaryLogicalOperation andOperation = new TDengineBinaryLogicalOperation(leftComparison,
                rightComparison, TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);
        if (isSymmetric) {
            TDengineBinaryComparisonOperation leftComparison2 = new TDengineBinaryComparisonOperation(right, expr,
                    TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
            TDengineBinaryComparisonOperation rightComparison2 = new TDengineBinaryComparisonOperation(expr, left,
                    TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
            TDengineBinaryLogicalOperation andOperation2 = new TDengineBinaryLogicalOperation(leftComparison2,
                    rightComparison2, TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);
            TDengineBinaryLogicalOperation orOp = new TDengineBinaryLogicalOperation(andOperation, andOperation2,
                    TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

}
