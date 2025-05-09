package com.benchmark.constants;

public enum DataBaseType {
    TSDB((short) 1), IOTDB((short) 2), INFLUXDB((short) 3);

    private short value;

    DataBaseType(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }

    public void setValue(short value) {
        this.value = value;
    }
}
