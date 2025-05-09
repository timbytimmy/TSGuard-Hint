package com.fuzzy.influxdb.ast;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.ast.BinaryOperatorNode.Operator;
import com.fuzzy.common.ast.UnaryOperatorNode;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.influxdb.ast.InfluxDBUnaryPrefixOperation.InfluxDBUnaryPrefixOperator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBUnaryPrefixOperation extends UnaryOperatorNode<InfluxDBExpression, InfluxDBUnaryPrefixOperator>
        implements InfluxDBExpression {

    public enum InfluxDBUnaryPrefixOperator implements Operator {
        PLUS("+") {
            @Override
            public InfluxDBConstant applyNotNull(InfluxDBConstant expr) {
                if (expr.isInt()) return InfluxDBConstant.createNoSuffixIntConstant(expr.getInt());
                else if (expr.isBoolean()) throw new IgnoreMeException();
                else return expr;
            }
        },
        MINUS("-") {
            @Override
            public InfluxDBConstant applyNotNull(InfluxDBConstant expr) {
                if (expr.isInt()) {
                    if (!expr.isSigned()) throw new IgnoreMeException();
                    return InfluxDBConstant.createNoSuffixIntConstant(-expr.getInt());
                } else if (expr.isDouble())
                    return InfluxDBConstant.createDoubleConstant(-expr.getDouble());
                else if (expr.isBigDecimal()) {
                    return InfluxDBConstant.createBigDecimalConstant(expr.getBigDecimalValue().negate());
                } else if (expr.isBoolean()) {
                    throw new IgnoreMeException();
                } else if (expr.isUInt() || expr.isString()) {
                    throw new IgnoreMeException();
                } else throw new AssertionError(expr);
            }
        };

        private String[] textRepresentations;

        InfluxDBUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract InfluxDBConstant applyNotNull(InfluxDBConstant expr);

        public static InfluxDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(InfluxDBUnaryPrefixOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public InfluxDBUnaryPrefixOperation(InfluxDBExpression expr, InfluxDBUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public void checkSyntax() {
        // expected identifier, number, duration
        if (expr.getExpectedValue().isString())
            throw new ReGenerateExpressionException("一元前缀表达式不支持字符串类型");
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

}
