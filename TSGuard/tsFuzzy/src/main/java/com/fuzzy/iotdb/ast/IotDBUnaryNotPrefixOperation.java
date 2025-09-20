package com.fuzzy.iotdb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;

public class IotDBUnaryNotPrefixOperation extends UnaryOperatorNode<IotDBExpression,
        IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator>
        implements IotDBExpression {

    public enum IotDBUnaryNotPrefixOperator implements Operator {
        NOT("TRUE != ") {
            @Override
            public IotDBConstant applyNotNull(IotDBConstant expr) {
                return IotDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_INT(" != ") {
            @Override
            public IotDBConstant applyNotNull(IotDBConstant expr) {
                return IotDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_DOUBLE(" >= <= ") {
            @Override
            public IotDBConstant applyNotNull(IotDBConstant expr) {
                return IotDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        };

        private String[] textRepresentations;

        IotDBUnaryNotPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract IotDBConstant applyNotNull(IotDBConstant expr);

        public static IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator getRandom(IotDBExpression subExpr) {
            IotDBExpression exprType = getNotPrefixTypeExpression(subExpr);

            IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator operator;
            if (exprType instanceof IotDBConstant && ((IotDBConstant) exprType).isBoolean())
                operator = IotDBUnaryNotPrefixOperator.NOT;
            else if (exprType instanceof IotDBConstant && ((IotDBConstant) exprType).isDouble())
                operator = IotDBUnaryNotPrefixOperator.NOT_DOUBLE;
            else operator = IotDBUnaryNotPrefixOperator.NOT_INT;
            return operator;
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public IotDBUnaryNotPrefixOperation(IotDBExpression expr, IotDBUnaryNotPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public IotDBConstant getExpectedValue() {
        IotDBConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return IotDBConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    public static IotDBUnaryNotPrefixOperation getNotUnaryPrefixOperation(IotDBExpression expr) {
        IotDBExpression exprType = getNotPrefixTypeExpression(expr);

        if (exprType instanceof IotDBConstant && ((IotDBConstant) exprType).isBoolean())
            return new IotDBUnaryNotPrefixOperation(expr, IotDBUnaryNotPrefixOperator.NOT);
        else if (exprType instanceof IotDBConstant && ((IotDBConstant) exprType).isDouble())
            return new IotDBUnaryNotPrefixOperation(expr, IotDBUnaryNotPrefixOperator.NOT_DOUBLE);
        else
            return new IotDBUnaryNotPrefixOperation(expr, IotDBUnaryNotPrefixOperator.NOT_INT);
    }

    private static IotDBExpression getNotPrefixTypeExpression(IotDBExpression expression) {
        IotDBExpression exprType;
        // 一元操作符Not类型取决于表达式结果，列引用取决于其值类型
        // 二元位操作取决于返回值, cast取决于强转后类型, 二元逻辑、二元比较操作符、常量取决于返回值
        if (expression instanceof IotDBUnaryNotPrefixOperation)
            exprType = expression.getExpectedValue();
        else if (expression instanceof IotDBUnaryPrefixOperation)
            exprType = ((IotDBUnaryPrefixOperation) expression).getExpression().getExpectedValue();
        else if (expression instanceof IotDBColumnReference)
            exprType = ((IotDBColumnReference) expression).getValue();
        else exprType = expression.getExpectedValue();
        return exprType;
    }

}
