package generator;

import com.benchmark.commonUtil.DataGenerator;
import com.benchmark.entity.PointData;

import java.util.List;

public class GenerateCurrentPointDataTest {
    private static final String pointNamePrefix = "point_";

    public static void main(String[] args) {
        List<PointData> pointDataList = DataGenerator.generateCurrentPointData(pointNamePrefix, 50, 1,
                10, 10, 1, false);

        pointDataList.forEach(pointData -> System.out.println(pointData.toString()));
    }
}
