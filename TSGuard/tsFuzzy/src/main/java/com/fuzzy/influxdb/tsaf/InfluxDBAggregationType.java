package com.fuzzy.influxdb.tsaf;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.aggregation.AggregationType;

public enum InfluxDBAggregationType {
    COUNT(AggregationType.COUNT, true),
    MEAN(AggregationType.AVG, false),
    SUM(AggregationType.SUM, false),
    SPREAD(AggregationType.SPREAD, false),
    STDDEV(AggregationType.STDDEV, false);

    AggregationType aggregationType;
    boolean fillZero;

    InfluxDBAggregationType(AggregationType aggregationType, boolean fillZero) {
        this.aggregationType = aggregationType;
        this.fillZero = fillZero;
    }

    public boolean isFillZero() {
        return fillZero;
    }

    public AggregationType getAggregationType() {
        return this.aggregationType;
    }

    public static InfluxDBAggregationType getInfluxDBAggregationType(AggregationType aggregationType) {
        for (InfluxDBAggregationType value : InfluxDBAggregationType.values()) {
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
            AggregationType.STDDEV,
    };
}
