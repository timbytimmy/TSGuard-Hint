package com.benchmark.constants;

public enum OperationType {
    // 插入操作(1~100)
    InsertHistoricalRangeData((short) 1), InsertHistoricalInstantData((short) 2),
    InsertRealtimeRandomData((short) 3),

    // 查询操作(101~200)
    QueryMultiPointHistRangeData((short) 101), QuerySinglePointHistRangeData((short) 102),        // 历史范围查询
    QueryMultiPointLastData((short) 103), QuerySinglePointLastData((short) 104),                  // 最近点查询
    QueryMultiPointRTData((short) 105), QuerySinglePointRTData((short) 106),                      // 实时值查询
    QueryMultiPointHistInstantData((short) 107), QuerySinglePointHistInstantData((short) 108),    // 历史断面查询

    // 聚合查询(121~140)
    MAX((short) 121), MIN((short) 122), AVG((short) 123), COUNT((short) 124),

    // 降采样查询(141~160)
    MaxDownSamplingQuery((short) 141), MinDownSamplingQuery((short) 142),
    AvgDownSamplingQuery((short) 143), CountDownSamplingQuery((short) 144);

    private short value;

    OperationType(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }

    public void setValue(short value) {
        this.value = value;
    }
}
