package com.benchmark.constants;

public enum ValueStatus {
    VALID((short) 1), INVALID((short) 0);

    private short value;

    ValueStatus(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }

    public void setValue(short value) {
        this.value = value;
    }
}
