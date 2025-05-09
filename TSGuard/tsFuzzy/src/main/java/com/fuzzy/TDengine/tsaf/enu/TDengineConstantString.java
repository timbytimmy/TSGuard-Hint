package com.fuzzy.TDengine.tsaf.enu;

public enum TDengineConstantString {
    TIME_FIELD_NAME("time"),
    W_START_TIME_COLUMN_NAME("_wstart"),
    DEVICE_ID_COLUMN_NAME("deviceId"),
    REF("ref"),
    ;

    private String name;

    TDengineConstantString(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
