package com.fuzzy.griddb.tsaf.enu;

public enum GridDBConstantString {
    DATABASE_NAME("public"),
    CLUSTER_NAME("myCluster"),
    TIME_FIELD_NAME("time"),
    DEVICE_ID_COLUMN_NAME("deviceId");

    private String name;

    GridDBConstantString(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
