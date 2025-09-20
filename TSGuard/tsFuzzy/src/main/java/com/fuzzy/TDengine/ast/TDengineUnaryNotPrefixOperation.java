package com.fuzzy.TDengine.ast;


import com.fuzzy.Randomly;
import com.fuzzy.TDengine.ast.TDengineUnaryNotPrefixOperation.TDengineUnaryNotPrefixOperator;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.common.gen.ReGenerateExpressionException;

public class TDengineUnaryNotPrefixOperation extends UnaryOperatorNode<TDengineExpression, TDengineUnaryNotPrefixOperator>
        implements TDengineExpression {

    public enum TDengineUnaryNotPrefixOperator implements Operator {
        NOT("NOT") {
            @Override
            public TDengineConstant applyNotNull(TDengineConstant expr) {
                return TDengineConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_DOUBLE(" = ") {
            @Override
            public TDengineConstant applyNotNull(TDengineConstant expr) {
                return TDengineConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_INT(" = ") {
            @Override
            public TDengineConstant applyNotNull(TDengineConstant expr) {
                return TDengineConstant.createBoolean(!expr.asBooleanNotNull());
            }
        };

        private String[] textRepresentations;

        TDengineUnaryNotPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract TDengineConstant applyNotNull(TDengineConstant expr);

        public static TDengineUnaryNotPrefixOperator getRandom(TDengineExpression subExpr) {
            TDengineExpression exprType = getNotPrefixTypeExpression(subExpr);

            TDengineUnaryNotPrefixOperator operator;
            if (exprType instanceof TDengineConstant && ((TDengineConstant) exprType).isBoolean())
                operator = TDengineUnaryNotPrefixOperator.NOT;
            else if (exprType instanceof TDengineConstant && ((TDengineConstant) exprType).isDouble())
                operator = TDengineUnaryNotPrefixOperator.NOT_DOUBLE;
            else operator = TDengineUnaryNotPrefixOperator.NOT_INT;
            return operator;
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public TDengineUnaryNotPrefixOperation(TDengineExpression expr, TDengineUnaryNotPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public void checkSyntax() {
        if (expr instanceof TDengineCastOperation)
            throw new ReGenerateExpressionException("NOT后不支持CAST");
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return TDengineConstant.createTrue();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    public static TDengineUnaryNotPrefixOperation getNotUnaryPrefixOperation(TDengineExpression expr) {
        TDengineExpression exprType = getNotPrefixTypeExpression(expr);

        if (exprType instanceof TDengineConstant && ((TDengineConstant) exprType).isBoolean())
            return new TDengineUnaryNotPrefixOperation(expr, TDengineUnaryNotPrefixOperator.NOT);
        else if (exprType instanceof TDengineConstant && ((TDengineConstant) exprType).isDouble())
            return new TDengineUnaryNotPrefixOperation(expr, TDengineUnaryNotPrefixOperator.NOT_DOUBLE);
        else
            return new TDengineUnaryNotPrefixOperation(expr, TDengineUnaryNotPrefixOperator.NOT_INT);
    }

    private static TDengineExpression getNotPrefixTypeExpression(TDengineExpression expression) {
        TDengineExpression exprType;
        // 一元操作符Not类型/可计算函数 取决于表达式结果，列引用取决于其值类型
        // 二元位操作取决于返回值, cast取决于强转后类型, 二元逻辑、二元比较操作符、常量取决于返回值
        if (expression instanceof TDengineUnaryNotPrefixOperation || expression instanceof TDengineComputableFunction)
            exprType = expression.getExpectedValue();
        else if (expression instanceof TDengineUnaryPrefixOperation)
            exprType = ((TDengineUnaryPrefixOperation) expression).getExpression().getExpectedValue();
        else if (expression instanceof TDengineColumnReference)
            exprType = ((TDengineColumnReference) expression).getValue();
        else exprType = expression.getExpectedValue();
        return exprType;
    }

}
