package com.fuzzy.iotdb.ast;


import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IotDBBetweenOperation implements IotDBExpression {

    private final IotDBExpression expr;
    private final IotDBExpression left;
    private final IotDBExpression right;
    private final boolean isSymmetric;

    public IotDBBetweenOperation(IotDBExpression expr, IotDBExpression left, IotDBExpression right, boolean isSymmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        this.isSymmetric = isSymmetric;
    }

    public IotDBExpression getExpr() {
        return expr;
    }

    public IotDBExpression getLeft() {
        return left;
    }

    public IotDBExpression getRight() {
        return right;
    }

    @Override
    public void checkSyntax() {
        IotDBConstant exprExpectedValue = expr.getExpectedValue();
        IotDBConstant leftExpectedValue = left.getExpectedValue();
        IotDBConstant rightExpectedValue = right.getExpectedValue();
        // Msg: 301: The Type of three subExpression should be all Numeric or Text
        if (!(exprExpectedValue.isNumber() && leftExpectedValue.isNumber() && rightExpectedValue.isNumber()
                || exprExpectedValue.isString() && leftExpectedValue.isString() && rightExpectedValue.isString())) {
            throw new ReGenerateExpressionException("Between");
        }
        // Msg: 701: Constant column is not supported.
        if (!(expr instanceof IotDBColumnReference
                || left instanceof IotDBColumnReference
                || right instanceof IotDBColumnReference)) {
            throw new ReGenerateExpressionException("Between");
        }
    }

    @Override
    public IotDBConstant getExpectedValue() {
        IotDBBinaryComparisonOperation leftComparison = new IotDBBinaryComparisonOperation(left, expr,
                IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        IotDBBinaryComparisonOperation rightComparison = new IotDBBinaryComparisonOperation(expr, right,
                IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        IotDBBinaryLogicalOperation andOperation = new IotDBBinaryLogicalOperation(leftComparison,
                rightComparison, IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND);
        if (isSymmetric) {
            IotDBBinaryComparisonOperation leftComparison2 = new IotDBBinaryComparisonOperation(right, expr,
                    IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
            IotDBBinaryComparisonOperation rightComparison2 = new IotDBBinaryComparisonOperation(expr, left,
                    IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
            IotDBBinaryLogicalOperation andOperation2 = new IotDBBinaryLogicalOperation(leftComparison2,
                    rightComparison2, IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND);
            IotDBBinaryLogicalOperation orOp = new IotDBBinaryLogicalOperation(andOperation, andOperation2,
                    IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

}
