package com.fuzzy.griddb.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.griddb.GridDBSchema.GridDBDataType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GridDBCastOperation implements GridDBExpression {

    private final GridDBExpression expr;
    private final CastType castType;

    public enum CastType {
        BOOL("BOOL"), STRING("STRING"), BYTE("BYTE"), SHORT("SHORT"),
        INTEGER("INTEGER"), LONG("LONG"), FLOAT("FLOAT"), DOUBLE("DOUBLE"),
        TIMESTAMP("TIMESTAMP"), GEOMETRY("GEOMETRY"), BLOB("BLOB"),
        BIGDECIMAL("BIGDECIMAL"), NULL("NULL");

        private String type;

        CastType(String type) {
            this.type = type;
        }

        public String getType() {
            return this.type;
        }

        public static CastType getRandom(GridDBDataType dataType) {
            // INT, UINT, BIGINT, UBIGINT, BINARY, VARCHAR, DOUBLE, BOOL
            switch (dataType) {
                case BOOL:
                case INTEGER:
                    return Randomly.fromOptions(INTEGER, LONG, DOUBLE, BOOL, STRING);
                case LONG:
                    return Randomly.fromOptions(LONG, DOUBLE, BOOL, STRING);
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return Randomly.fromOptions(DOUBLE, BOOL);
                case STRING:
                    return Randomly.fromOptions(BOOL, STRING);
                case NULL:
                    throw new IgnoreMeException();
                default:
                    throw new AssertionError();
            }
        }

        public static CastType GridDBDataTypeToCastType(GridDBDataType dataType) {
            for (CastType value : CastType.values()) {
                if (value.getType().equals(dataType.getTypeName())) return value;
            }
            throw new IllegalArgumentException(
                    String.format("该GridDBDataType不能转换为CastType, dataType:%s", dataType));
        }
    }

    public GridDBCastOperation(GridDBExpression expr, CastType castType) {
        this.expr = expr;
        this.castType = castType;
    }

    public GridDBExpression getExpr() {
        return expr;
    }

    public String getCastType() {
        return castType.getType();
    }

    @Override
    public void checkSyntax() {
        if (!(expr instanceof GridDBColumnReference)) {
            throw new ReGenerateExpressionException("GridDBCastOperation");
        }
    }

    @Override
    public GridDBConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(GridDBDataType.getInstanceFromValue(castType.getType()));
    }

    @Override
    public boolean hasColumn() {
        return expr.hasColumn();
    }
}
