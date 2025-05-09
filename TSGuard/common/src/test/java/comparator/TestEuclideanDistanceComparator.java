package comparator;

import com.benchmark.comparator.TimeSeriesComparator;
import com.benchmark.comparator.TimeSeriesComparatorFactory;
import com.benchmark.comparator.enu.TimeSeriesComparatorType;
import com.benchmark.constants.DataType;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PointData;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TestEuclideanDistanceComparator {

    static List<PointData> pointDataListLeftTrue = new ArrayList<>();
    static List<PointData> pointDataListRightTrue = new ArrayList<>();
    static List<PointData> pointDataListLeftFalse = new ArrayList<>();
    static List<PointData> pointDataListRightFalse = new ArrayList<>();

    static {
        // 2024-03-04 00:00:00
        PointData pointDataLeft1 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481600000L)
                .withFieldValue(66.66)
                .build();
        // 2024-03-04 00:01:00
        PointData pointDataLeft2 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481660000L)
                .withFieldValue(67.66)
                .build();
        // 2024-03-04 00:02:00
        PointData pointDataLeft3 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481720000L)
                .withFieldValue(68.66649999999)
                .build();

        // 2024-03-04 00:00:00
        PointData pointDataRight1 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481600000L)
                .withFieldValue(66.66)
                .build();
        // 2024-03-04 00:01:00
        PointData pointDataRight2 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481660000L)
                .withFieldValue(67.66)
                .build();
        // 2024-03-04 00:02:00
        PointData pointDataRight3 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481720000L)
                .withFieldValue(68.666)
                .build();

        pointDataListLeftTrue.add(pointDataLeft1);
        pointDataListLeftTrue.add(pointDataLeft2);
        pointDataListLeftTrue.add(pointDataLeft3);
        pointDataListRightTrue.add(pointDataRight1);
        pointDataListRightTrue.add(pointDataRight2);
        pointDataListRightTrue.add(pointDataRight3);

        // 2024-03-04 00:02:00
        PointData pointDataLeft4 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481720000L)
                .withFieldValue(68.66650001)
                .build();
        // 2024-03-04 00:02:00
        PointData pointDataRight4 = PointData.PointDataBuilder.anPointData()
                .withUtcTime(1709481720000L)
                .withFieldValue(68.666)
                .build();

        pointDataListLeftFalse.add(pointDataLeft1);
        pointDataListLeftFalse.add(pointDataLeft2);
        pointDataListLeftFalse.add(pointDataLeft4);
        pointDataListRightFalse.add(pointDataRight1);
        pointDataListRightFalse.add(pointDataRight2);
        pointDataListRightFalse.add(pointDataRight4);
    }

    public static void main(String[] args) {
        TimeSeriesComparator tsComparator = TimeSeriesComparatorFactory.getTSComparator(
                TimeSeriesComparatorType.EUCLIDEAN_DISTANCE);
        boolean compareTrue = tsComparator.compare(pointDataListLeftTrue, pointDataListRightTrue);
        boolean compareFalse = tsComparator.compare(pointDataListLeftFalse, pointDataListRightFalse);

        assert compareTrue;
        assert !compareFalse;
    }

    @Test
    public void testDBValToString() {
        DBVal dbVal = DBVal.DBValBuilder.anDBVal()
                .withUtcTime(1709481720000L)
                .withTableName("tableName")
                .withTagName("tagName")
                .withTagValue("tagValue")
                .build();
        dbVal.addField("fieldName1", DataType.FLOAT, 1);
        dbVal.addField("fieldName2", DataType.FLOAT, 2);
        dbVal.addField("fieldName3", DataType.FLOAT, 3);

        log.info("dbVal:{}", dbVal);
    }

}
