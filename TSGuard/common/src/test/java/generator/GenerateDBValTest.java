package generator;

import com.benchmark.commonUtil.DataGenerator;
import com.benchmark.entity.DBVal;

import java.util.List;

public class GenerateDBValTest {
    private static final String pointNamePrefix = "point_";

    public static void main(String[] args) {
        List<DBVal> dbVals = DataGenerator.generateDBVal(pointNamePrefix, 10, 1,
                5, System.currentTimeMillis(), 7000, 10,
                10, 1, false);

        dbVals.forEach(pointData -> System.out.println(pointData.toString()));
    }
}
