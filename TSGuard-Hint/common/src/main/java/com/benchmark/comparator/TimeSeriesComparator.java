package com.benchmark.comparator;

import com.benchmark.entity.PointData;

import java.util.List;

public abstract class TimeSeriesComparator {

    public abstract boolean compare(List<PointData> pointDataListLeft, List<PointData> pointDataListRight);

}
