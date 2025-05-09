package com.fuzzy.TDengine.ast;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDengineBinaryLogicalOperation implements TDengineExpression {

    private final TDengineExpression left;
    private final TDengineExpression right;
    private final TDengineBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum TDengineBinaryLogicalOperator {
        AND("AND") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                if (left.isNull() || right.isNull()) return TDengineConstant.createNullConstant();
                else return TDengineConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
            }
        },
        OR("OR") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                if (left.isNull() && right.isNull()) return TDengineConstant.createNullConstant();
                else if (left.isNull()) return TDengineConstant.createBoolean(right.asBooleanNotNull());
                else if (right.isNull()) return TDengineConstant.createBoolean(left.asBooleanNotNull());
                else return TDengineConstant.createBoolean(left.asBooleanNotNull() || right.asBooleanNotNull());
            }
        };

        private final String[] textRepresentations;

        TDengineBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract TDengineConstant apply(TDengineConstant left, TDengineConstant right);

        public static TDengineBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static TDengineBinaryLogicalOperator getOperatorByText(String text) {
            for (TDengineBinaryLogicalOperator value : TDengineBinaryLogicalOperator.values()) {
                for (String textRepresentation : value.textRepresentations) {
                    if (textRepresentation.equalsIgnoreCase(text)) return value;
                }
            }
            throw new UnsupportedOperationException();
        }
    }

    public TDengineBinaryLogicalOperation(TDengineExpression left, TDengineExpression right, TDengineBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public TDengineExpression getLeft() {
        return left;
    }

    public TDengineBinaryLogicalOperator getOp() {
        return op;
    }

    public TDengineExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant leftExpected = left.getExpectedValue();
        TDengineConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

    @Override
    public void checkSyntax() {
        if (!(left.getExpectedValue().isBoolean() && right.getExpectedValue().isBoolean())) {
            log.warn("dataType is not BOOL");
            throw new ReGenerateExpressionException("Logic");
        }
    }
}
