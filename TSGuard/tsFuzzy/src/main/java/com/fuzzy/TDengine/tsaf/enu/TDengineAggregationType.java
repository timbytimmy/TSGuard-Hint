package com.fuzzy.TDengine.tsaf.enu;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.aggregation.AggregationType;

public enum TDengineAggregationType {
    SUM(AggregationType.SUM, false),
    COUNT(AggregationType.COUNT, true),
    AVG(AggregationType.AVG, false),
    SPREAD(AggregationType.SPREAD, false),
    STDDEV_POP(AggregationType.STDDEV_POP, false),
    VAR_POP(AggregationType.VAR_POP, false),

    MAX(AggregationType.MAX, false),
    MIN(AggregationType.MIN, false),
    FIRST(AggregationType.FIRST, false),
    LAST(AggregationType.LAST, false),
    ;

    AggregationType aggregationType;
    boolean fillZero;

    TDengineAggregationType(AggregationType aggregationType, boolean fillZero) {
        this.aggregationType = aggregationType;
        this.fillZero = fillZero;
    }

    public AggregationType getAggregationType() {
        return this.aggregationType;
    }

    public boolean isFillZero() {
        return fillZero;
    }

    public static TDengineAggregationType getTDengineAggregationType(AggregationType aggregationType) {
        for (TDengineAggregationType value : TDengineAggregationType.values()) {
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
            AggregationType.SPREAD,
            AggregationType.STDDEV_POP,
//            AggregationType.VAR_POP,

            AggregationType.MAX,
            AggregationType.MIN,
            AggregationType.FIRST,
            AggregationType.LAST,
    };
}
