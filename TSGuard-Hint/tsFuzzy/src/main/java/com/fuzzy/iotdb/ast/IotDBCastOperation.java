package com.fuzzy.iotdb.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.iotdb.IotDBSchema.IotDBDataType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IotDBCastOperation implements IotDBExpression {

    private final IotDBExpression expr;
    private final CastType castType;

    public enum CastType {
        INT32("INT32"), INT64("INT64"), FLOAT("FLOAT"), DOUBLE("DOUBLE"),
        BOOLEAN("BOOLEAN"), TEXT("TEXT"), BIGDECIMAL("BIGDECIMAL");

        private String type;

        CastType(String type) {
            this.type = type;
        }

        public String getType() {
            return this.type;
        }

        public static CastType getRandom(IotDBDataType dataType) {
            switch (dataType) {
                case INT64:
                    return Randomly.fromOptions(INT64, FLOAT, DOUBLE, BOOLEAN, TEXT);
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return Randomly.fromOptions(FLOAT, DOUBLE, TEXT, BOOLEAN);
                case INT32:
                case TEXT:
                case BOOLEAN:
                    return Randomly.fromOptions(INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT);
                case NULL:
                    throw new IgnoreMeException();
                default:
                    throw new AssertionError();
            }
        }

        public static CastType iotDBDataTypeToCastType(IotDBDataType dataType) {
            switch (dataType) {
                case DOUBLE:
                    return CastType.DOUBLE;
                case FLOAT:
                    return CastType.FLOAT;
                case BOOLEAN:
                    return CastType.BOOLEAN;
                case TEXT:
                    return CastType.TEXT;
                case INT64:
                    return CastType.INT64;
                case INT32:
                    return CastType.INT32;
                default:
                    throw new IllegalArgumentException(
                            String.format("该IotDBDataType不能转换为CastType, dataType:%s", dataType));
            }
        }
    }

    public IotDBCastOperation(IotDBExpression expr, CastType castType) {
        this.expr = expr;
        this.castType = castType;
    }

    public IotDBExpression getExpr() {
        return expr;
    }

    public String getCastType() {
        return castType.getType();
    }

    @Override
    public void checkSyntax() {
        if (!(expr instanceof IotDBColumnReference)) {
            throw new ReGenerateExpressionException("IotDBCastOperation");
        }
    }

    @Override
    public IotDBConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(castType);
    }

}
