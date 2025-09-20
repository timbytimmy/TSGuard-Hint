package com.fuzzy.common.coverage;

public enum DatabaseTypeForTransCode {
    TDengine(".py"), InfluxDB(".");

    private String targetFileType;
    DatabaseTypeForTransCode(String targetFileType) {
        this.targetFileType = targetFileType;
    }

    public String getTargetFileType() {
        return this.targetFileType;
    }
}
