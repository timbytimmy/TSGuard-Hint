package generator;

import com.benchmark.commonUtil.DataGenerator;
import com.benchmark.dto.DBValParam;

import java.util.List;

public class GenerateDBValParamTest {
    private static final String pointNamePrefix = "point_";

    public static void main(String[] args) {
        List<DBValParam> dbValParamList = DataGenerator.generateDBValParams(pointNamePrefix, 50, 1);

        dbValParamList.forEach(dbValParam -> System.out.println(dbValParam.toString()));
    }
}
