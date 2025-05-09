package com.fuzzy.TDengine.ast;


import com.fuzzy.Randomly;
import com.fuzzy.TDengine.ast.TDengineCastOperation.CastType;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BinaryOperator;

@Slf4j
public class TDengineBinaryOperation implements TDengineExpression {

    private final TDengineExpression left;
    private final TDengineExpression right;
    private final TDengineBinaryOperator op;

    public enum TDengineBinaryOperator {
        AND("&") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public TDengineConstant apply(TDengineConstant left, TDengineConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        };

        private String textRepresentation;

        private static TDengineConstant applyBitOperation(TDengineConstant left, TDengineConstant right,
                                                          BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return TDengineConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(CastType.BIGINT).getInt();
                long rightVal = right.castAs(CastType.BIGINT).getInt();
                long value = op.apply(leftVal, rightVal);
                return TDengineConstant.createInt64Constant(value);
            }
        }

        TDengineBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract TDengineConstant apply(TDengineConstant left, TDengineConstant right);

        public static TDengineBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public TDengineBinaryOperation(TDengineExpression left, TDengineExpression right, TDengineBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public void checkSyntax() {
        if (!(left.getExpectedValue().isInt() && right.getExpectedValue().isInt()))
            throw new ReGenerateExpressionException("按位运算仅支持整型");
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant leftExpected = left.getExpectedValue();
        TDengineConstant rightExpected = right.getExpectedValue();
        return op.apply(leftExpected, rightExpected);
    }

    public TDengineExpression getLeft() {
        return left;
    }

    public TDengineBinaryOperator getOp() {
        return op;
    }

    public TDengineExpression getRight() {
        return right;
    }

}
