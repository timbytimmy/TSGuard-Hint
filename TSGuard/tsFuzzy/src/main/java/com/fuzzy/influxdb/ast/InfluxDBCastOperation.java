package com.fuzzy.influxdb.ast;

import com.fuzzy.Randomly;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBCastOperation implements InfluxDBExpression {

    private final InfluxDBExpression expr;
    private final CastType castType;

    public enum CastType {
        SIGNED("integer"), UNSIGNED("unsigned"), FLOAT("float"), STRING("string"),
        INTEGER("integer"), BOOLEAN("boolean"), FIELD("field"), TAG("tag"), BIGDECIMAL("bigDecimal");

        private String type;

        CastType(String type) {
            this.type = type;
        }

        public String getType() {
            return this.type;
        }

        public static CastType getRandom(InfluxDBColumn column) {
            // unsigned -> float, field, unsigned
            // float -> float, field
            // integer -> float, field
            // tag -> tag
            if (!column.isTag()) {
                switch (column.getType()) {
                    case FLOAT:
                    case INT:
                        return Randomly.fromOptions(FLOAT, FIELD);
                    case UINT:
                        return Randomly.fromOptions(UNSIGNED, FLOAT, FIELD);
                    case STRING:
                        return Randomly.fromOptions(STRING, FIELD);
                    case BOOLEAN:
                        return Randomly.fromOptions(FIELD);
                    default:
                        throw new AssertionError();
                }
            } else return Randomly.fromOptions(TAG);
        }
    }

    public InfluxDBCastOperation(InfluxDBExpression expr, CastType castType) {
        this.expr = expr;
        this.castType = castType;
    }

    public InfluxDBExpression getExpr() {
        return expr;
    }

    public String getCastType() {
        return castType.getType();
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(castType);
    }

}
