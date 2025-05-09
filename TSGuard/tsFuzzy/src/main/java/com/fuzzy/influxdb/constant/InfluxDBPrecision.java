package com.fuzzy.influxdb.constant;

import com.fuzzy.Randomly;

public enum InfluxDBPrecision {
    // TODO us 插入时间戳异常, 会被扩充至ns级别
    ns, ms, s;

    public static InfluxDBPrecision getRandom() {
        return Randomly.fromOptions(values());
    }
}
