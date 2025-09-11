package com.benchmark.fieldName.instance;

import com.benchmark.constants.DataType;

public enum DriverLocationFieldName {
    // 时间戳及TagName
    TIMESTAMP("createTime", DataType.TEXT),
    TAG("driverId", DataType.TEXT),
    // 若干字段
    DIRECTION("direction", DataType.DOUBLE),
    ELEVATION("elevation", DataType.INT32),
    LAT("lat", DataType.DOUBLE),
    LNG("lng", DataType.DOUBLE),
    SPEED("speed", DataType.DOUBLE);

    private String name;
    private DataType type;

    DriverLocationFieldName(String name, DataType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getType() {
        return this.type;
    }

    public void setType(DataType type) {
        this.type = type;
    }
}
