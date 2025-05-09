package generator;

import com.benchmark.commonUtil.DataGenerator;
import com.benchmark.entity.DBVal;

import java.util.List;

public class GenerateCurrentDBValTest {
    private static final String pointNamePrefix = "point_";

    public static void main(String[] args) {
        List<DBVal> dbVals = DataGenerator.generateCurrentDBVal(pointNamePrefix, 50, 1,
                10, 10, 1, false);

        dbVals.forEach(pointData -> System.out.println(pointData.toString()));
    }
}
