package com.benchmark.comparator;

import com.benchmark.comparator.enu.TimeSeriesComparatorType;

public class TimeSeriesComparatorFactory {

    public static TimeSeriesComparator getTSComparator(TimeSeriesComparatorType timeSeriesComparatorType) {
        switch (timeSeriesComparatorType) {
            case EUCLIDEAN_DISTANCE:
                return new EuclideanDistanceComparator();
            default:
                throw new IllegalArgumentException(String.format("时间序列比较器类型不合法: %s", timeSeriesComparatorType));
        }
    }

}
