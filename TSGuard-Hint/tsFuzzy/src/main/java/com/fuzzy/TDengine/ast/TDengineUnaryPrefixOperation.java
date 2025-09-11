package com.fuzzy.TDengine.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineSchema.TDengineDataType;
import com.fuzzy.TDengine.ast.TDengineUnaryPrefixOperation.TDengineUnaryPrefixOperator;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;

public class TDengineUnaryPrefixOperation extends UnaryOperatorNode<TDengineExpression, TDengineUnaryPrefixOperator>
        implements TDengineExpression {

    public enum TDengineUnaryPrefixOperator implements Operator {
        PLUS("+") {
            @Override
            public TDengineConstant applyNotNull(TDengineConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public TDengineConstant applyNotNull(TDengineConstant expr) {
                if (expr.isString() || expr.isBoolean()) throw new IgnoreMeException();
                else if (expr.isInt()) {
                    if (!expr.isSigned()) throw new IgnoreMeException();
                    if (TDengineDataType.INT.equals(expr.getType()))
                        return TDengineConstant.createInt32Constant(-expr.getInt());
                    else if (TDengineDataType.BIGINT.equals(expr.getType()))
                        return TDengineConstant.createInt64Constant(-expr.getInt());
                    else throw new AssertionError(expr);
                } else if (expr.isDouble()) return TDengineConstant.createDoubleConstant(-expr.getDouble());
                else if (expr.isBigDecimal()) return TDengineConstant.createBigDecimalConstant(
                        expr.getBigDecimalValue().negate());
                else throw new AssertionError(expr);

            }
        };

        private String[] textRepresentations;

        TDengineUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract TDengineConstant applyNotNull(TDengineConstant expr);

        public static TDengineUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public TDengineUnaryPrefixOperation(TDengineExpression expr, TDengineUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return TDengineConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
