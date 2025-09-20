package insert;

import com.benchmark.commonUtil.GetDBValFromFile;
import com.benchmark.entity.DBVal;
import com.benchmark.fieldName.FieldName;
import com.benchmark.fieldName.instance.DriverLocationFieldName;

import java.util.List;

public class ImportData {
    public final static String JSONFILEPATH = "F:\\T3_Data\\JSON\\50.json";

    public static void main(String[] args) {
        List<DBVal> dbValList =
                GetDBValFromFile.getPointDataFromJSON(JSONFILEPATH, new FieldName(DriverLocationFieldName.values()),
                        "driverLocation");

        dbValList.forEach(dbVal -> System.out.println(dbVal.toString()));
    }
}
