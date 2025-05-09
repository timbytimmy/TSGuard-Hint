package com.fuzzy.griddb.tsaf.enu;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.aggregation.AggregationType;

public enum GridDBAggregationType {
    SUM(AggregationType.SUM, false),
    COUNT(AggregationType.COUNT, true),
    AVG(AggregationType.AVG, false),
    STDDEV0(AggregationType.STDDEV, false),
    STDDEV_POP(AggregationType.STDDEV_POP, false),
    VAR_POP(AggregationType.VAR_POP, false),

    MAX(AggregationType.MAX, false),
    MIN(AggregationType.MIN, false),
    ;

    AggregationType aggregationType;
    boolean fillZero;

    GridDBAggregationType(AggregationType aggregationType, boolean fillZero) {
        this.aggregationType = aggregationType;
        this.fillZero = fillZero;
    }

    public AggregationType getAggregationType() {
        return this.aggregationType;
    }

    public boolean isFillZero() {
        return fillZero;
    }

    public static GridDBAggregationType getGridDBAggregationType(AggregationType aggregationType) {
        for (GridDBAggregationType value : GridDBAggregationType.values()) {
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
            AggregationType.STDDEV,
            AggregationType.STDDEV_POP,
//            AggregationType.VAR_POP,

            AggregationType.MAX,
            AggregationType.MIN,
    };
}
