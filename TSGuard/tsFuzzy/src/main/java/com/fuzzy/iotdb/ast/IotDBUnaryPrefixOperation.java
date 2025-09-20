package com.fuzzy.iotdb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.iotdb.ast.IotDBUnaryPrefixOperation.IotDBUnaryPrefixOperator;

public class IotDBUnaryPrefixOperation extends UnaryOperatorNode<IotDBExpression, IotDBUnaryPrefixOperator>
        implements IotDBExpression {

    public enum IotDBUnaryPrefixOperator implements Operator {
        PLUS("+") {
            @Override
            public IotDBConstant apply(IotDBConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public IotDBConstant apply(IotDBConstant expr) {
                if (expr.isString()) throw new IgnoreMeException();
                else if (expr.isDouble()) return IotDBConstant.createDoubleConstant(-expr.getDouble());
                else if (expr.isInt()) {
                    if (!expr.isSigned()) throw new IgnoreMeException();
                    return IotDBConstant.createIntConstant(-expr.getInt());
                } else if (expr.isBigDecimal()) {
                    return IotDBConstant.createBigDecimalConstant(expr.getBigDecimalValue().negate());
                } else if (expr.isBoolean()) {
                    // Msg: org.apache.iotdb.jdbc.IoTDBSQLException: 305: [INTERNAL_SERVER_ERROR(305)] Exception occurred: "select * from root.* where t1 - TURE". executeStatement failed. Unsupported expression type: SUBTRACTION
                    throw new IgnoreMeException();
                } else throw new AssertionError(expr);
            }
        };

        private String[] textRepresentations;

        IotDBUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract IotDBConstant apply(IotDBConstant expr);

        public static IotDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public IotDBUnaryPrefixOperation(IotDBExpression expr, IotDBUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public IotDBConstant getExpectedValue() {
        IotDBConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return IotDBConstant.createNullConstant();
        } else {
            return op.apply(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

    @Override
    public void checkSyntax() {
        if (this.expr.getExpectedValue().isBoolean() && this.op == IotDBUnaryPrefixOperator.MINUS) {
            // E: org.apache.iotdb.jdbc.IoTDBSQLException: 701: Invalid input expression data type. expression: root.db0.t1 < 2, actual data type: BOOLEAN, expected data type(s): [INT32, INT64, FLOAT, DOUBLE].
            throw new ReGenerateExpressionException("IotDBUnaryPrefixOperation");
        }
    }
}
