package com.fuzzy.TDengine.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineSchema.TDengineDataType;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDengineCastOperation implements TDengineExpression {

    private final TDengineExpression expr;
    private final CastType castType;

    public enum CastType {
        INT("INT"), UINT("INT UNSIGNED"), TIMESTAMP("TIMESTAMP"),
        BIGINT("BIGINT"), UBIGINT("BIGINT UNSIGNED"), FLOAT("FLOAT"),
        DOUBLE("DOUBLE"), BINARY("BINARY"), SMALLINT("SMALLINT"),
        USMALLINT("SMALLINT UNSIGNED"), TINYINT("TINYINT"), UTINYINT("TINYINT UNSIGNED"),
        BOOL("BOOL"), NCHAR("NCHAR"), JSON("JSON"),
        VARCHAR("VARCHAR"), GEOMETRY("GEOMETRY"), VARBINARY("VARBINARY"),
        NULL("NULL"), BIGDECIMAL("BigDecimal");

        private String type;

        CastType(String type) {
            this.type = type;
        }

        public String getType() {
            return this.type;
        }

        public static CastType getRandom(TDengineDataType dataType) {
            // INT, UINT, BIGINT, UBIGINT, BINARY, VARCHAR, DOUBLE, BOOL
            switch (dataType) {
                case BOOL:
                case INT:
                case UINT:
                    return Randomly.fromOptions(INT, UINT, BIGINT, UBIGINT, DOUBLE, BOOL, VARCHAR, BINARY);
                case BIGINT:
                case UBIGINT:
                    return Randomly.fromOptions(BIGINT, UBIGINT, DOUBLE, BOOL, VARCHAR, BINARY);
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return Randomly.fromOptions(DOUBLE, BOOL, VARCHAR, BINARY);
                case BINARY:
                case VARCHAR:
                    return Randomly.fromOptions(BOOL, VARCHAR, BINARY);
                case NULL:
                    throw new IgnoreMeException();
                default:
                    throw new AssertionError();
            }
        }

        public static CastType TDengineDataTypeToCastType(TDengineDataType dataType) {
            for (CastType value : CastType.values()) {
                if (value.getType().equals(dataType.getTypeName())) return value;
            }
            throw new IllegalArgumentException(
                    String.format("该TDengineDataType不能转换为CastType, dataType:%s", dataType));
        }
    }

    public TDengineCastOperation(TDengineExpression expr, CastType castType) {
        this.expr = expr;
        this.castType = castType;
    }

    public TDengineExpression getExpr() {
        return expr;
    }

    public String getCastType() {
        return castType.getType();
    }

    @Override
    public void checkSyntax() {
        if (!(expr instanceof TDengineColumnReference)) {
            throw new ReGenerateExpressionException("TDengineCastOperation");
        }
    }

    @Override
    public TDengineConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(castType);
    }

}
