package com.benchmark.comparator;

import com.benchmark.entity.PointData;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;

@Slf4j
public class EuclideanDistanceComparator extends TimeSeriesComparator {

    @Override
    public boolean compare(List<PointData> pointDataListLeft, List<PointData> pointDataListRight) {
        if (pointDataListLeft.size() != pointDataListRight.size()) return false;
        pointDataListLeft.sort(new Comparator<PointData>() {
            @Override
            public int compare(PointData o1, PointData o2) {
                return Long.compare(o1.getUtcTime(), o2.getUtcTime());
            }
        });
        pointDataListRight.sort(new Comparator<PointData>() {
            @Override
            public int compare(PointData o1, PointData o2) {
                return Long.compare(o1.getUtcTime(), o2.getUtcTime());
            }
        });
        double euclideanDistance = 0;
        for (int i = 0; i < pointDataListLeft.size(); i++) {
            euclideanDistance += Math.pow(((double) pointDataListLeft.get(i).getFieldValue() -
                    (double)  pointDataListRight.get(i).getFieldValue()), 2);
        }
        return Math.sqrt(euclideanDistance) < 0.0005;
    }

}
