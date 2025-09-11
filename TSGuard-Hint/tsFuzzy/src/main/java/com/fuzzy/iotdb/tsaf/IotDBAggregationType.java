package com.fuzzy.iotdb.tsaf;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.aggregation.AggregationType;

public enum IotDBAggregationType {
    SUM(AggregationType.SUM, false),
    COUNT(AggregationType.COUNT, true),
    AVG(AggregationType.AVG, false),
    STDDEV(AggregationType.STDDEV, false),
    STDDEV_POP(AggregationType.STDDEV_POP, false),
    VAR_POP(AggregationType.VAR_POP, false),

    MAX_VALUE(AggregationType.MAX, false),
    MIN_VALUE(AggregationType.MIN, false),
    FIRST_VALUE(AggregationType.FIRST, false),
    LAST_VALUE(AggregationType.LAST, false),
    EXTREME(AggregationType.EXTREME, false),
    MAX_TIME(AggregationType.MAX_TIME, false),
    MIN_TIME(AggregationType.MIN_TIME, false),
    ;

    AggregationType aggregationType;
    boolean fillZero;

    IotDBAggregationType(AggregationType aggregationType, boolean fillZero) {
        this.aggregationType = aggregationType;
        this.fillZero = fillZero;
    }

    public AggregationType getAggregationType() {
        return this.aggregationType;
    }

    public boolean isFillZero() {
        return fillZero;
    }

    public static IotDBAggregationType getIotDBAggregationType(AggregationType aggregationType) {
        for (IotDBAggregationType value : IotDBAggregationType.values()) {
            if (value.getAggregationType().equals(aggregationType)) return value;
        }
        throw new IllegalArgumentException();
    }

    public static AggregationType getRandomAggregationType() {
        return Randomly.fromOptions(generalAggregationType);
    }

    static AggregationType[] generalAggregationType = {
            AggregationType.SUM,
            AggregationType.COUNT,
            AggregationType.AVG,
//            AggregationType.STDDEV,
//            AggregationType.STDDEV_POP,
//            AggregationType.VAR_POP,

            AggregationType.MAX,
            AggregationType.MIN,
            AggregationType.FIRST,
            AggregationType.LAST,
            AggregationType.EXTREME,
            AggregationType.MAX_TIME,
            AggregationType.MIN_TIME,
    };
}
