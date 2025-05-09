package com.fuzzy.iotdb.util;

public enum IotDBValueStateConstant {

    NULL("null"),
    TIME_FIELD("time"),
    TIMESTAMP("timestamp"),
    TRUE("true"),
    FALSE("false"),
//    FIELD_COLUMN("field")

    ;

    private String value;

    IotDBValueStateConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

}
