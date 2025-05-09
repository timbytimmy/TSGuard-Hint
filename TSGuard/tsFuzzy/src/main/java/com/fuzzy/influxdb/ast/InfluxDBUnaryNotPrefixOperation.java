package com.fuzzy.influxdb.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.influxdb.ast.InfluxDBUnaryNotPrefixOperation.InfluxDBUnaryNotPrefixOperator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBUnaryNotPrefixOperation extends UnaryOperatorNode<InfluxDBExpression, InfluxDBUnaryNotPrefixOperator>
        implements InfluxDBExpression {

    public enum InfluxDBUnaryNotPrefixOperator implements Operator {
        NOT_BOOLEAN("TRUE != ") {
            @Override
            public InfluxDBConstant applyNotNull(InfluxDBConstant expr) {
                return InfluxDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_STR(" != ") {
            @Override
            public InfluxDBConstant applyNotNull(InfluxDBConstant expr) {
                return InfluxDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_UINTEGER(" != ") {
            @Override
            public InfluxDBConstant applyNotNull(InfluxDBConstant expr) {
                return InfluxDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        },
        NOT_FLOAT(" >= <= ") {
            @Override
            public InfluxDBConstant applyNotNull(InfluxDBConstant expr) {
                return InfluxDBConstant.createBoolean(!expr.asBooleanNotNull());
            }
        };

        private String[] textRepresentations;

        InfluxDBUnaryNotPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract InfluxDBConstant applyNotNull(InfluxDBConstant expr);

        public static InfluxDBUnaryNotPrefixOperator getRandom(InfluxDBExpression subExpr) {
            InfluxDBExpression exprType = getNotPrefixTypeExpression(subExpr);

            InfluxDBUnaryNotPrefixOperator operator;
            if (exprType instanceof InfluxDBConstant && ((InfluxDBConstant) exprType).isBoolean())
                operator = InfluxDBUnaryNotPrefixOperator.NOT_BOOLEAN;
            else if (exprType instanceof InfluxDBConstant && ((InfluxDBConstant) exprType).isDouble())
                operator = InfluxDBUnaryNotPrefixOperator.NOT_FLOAT;
            else operator = Randomly.fromOptions(InfluxDBUnaryNotPrefixOperator.NOT_STR,
                        InfluxDBUnaryNotPrefixOperator.NOT_UINTEGER);
            return operator;
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public InfluxDBUnaryNotPrefixOperation(InfluxDBExpression expr, InfluxDBUnaryNotPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        InfluxDBConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return InfluxDBConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    public static InfluxDBUnaryNotPrefixOperation getNotUnaryPrefixOperation(InfluxDBExpression expr) {
        InfluxDBExpression exprType = getNotPrefixTypeExpression(expr);

        if (exprType instanceof InfluxDBConstant && ((InfluxDBConstant) exprType).isBoolean())
            return new InfluxDBUnaryNotPrefixOperation(expr, InfluxDBUnaryNotPrefixOperator.NOT_BOOLEAN);
        else if (exprType instanceof InfluxDBConstant && ((InfluxDBConstant) exprType).isDouble())
            return new InfluxDBUnaryNotPrefixOperation(expr, InfluxDBUnaryNotPrefixOperator.NOT_FLOAT);
        else
            return new InfluxDBUnaryNotPrefixOperation(expr, Randomly.fromOptions(InfluxDBUnaryNotPrefixOperator.NOT_STR,
                    InfluxDBUnaryNotPrefixOperator.NOT_UINTEGER));
    }

    private static InfluxDBExpression getNotPrefixTypeExpression(InfluxDBExpression expression) {
        InfluxDBExpression exprType;
        // 一元操作符Not类型取决于表达式结果，列引用取决于其值类型
        // 二元位操作取决于返回值, cast取决于强转后类型, 二元逻辑、二元比较操作符、常量取决于返回值
        if (expression instanceof InfluxDBUnaryNotPrefixOperation)
            exprType = expression.getExpectedValue();
        else if (expression instanceof InfluxDBUnaryPrefixOperation)
            exprType = ((InfluxDBUnaryPrefixOperation) expression).getExpression().getExpectedValue();
        else if (expression instanceof InfluxDBColumnReference)
            exprType = ((InfluxDBColumnReference) expression).getValue();
        else exprType = expression.getExpectedValue();
        return exprType;
    }

}
