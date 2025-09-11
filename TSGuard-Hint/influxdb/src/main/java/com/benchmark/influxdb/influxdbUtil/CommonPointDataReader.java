package com.benchmark.influxdb.influxdbUtil;

import com.benchmark.entity.DBVal;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

@Slf4j
public class CommonPointDataReader {
    private static String charSet = "utf-8";

    /**
     * @param commonPointDataHashTable
     * @return java.util.List<db.TempDBVal>
     * @description 将CommonPointData转为tsdb支持的DBVal执行插入操作
     * @dateTime 2023/2/4 10:43
     */
    public static List<DBVal> convertCommonPointDataToDBVal(
            Hashtable<String, List<DBVal>> commonPointDataHashTable) {
        List<DBVal> dbValList = new ArrayList<>();
        if (commonPointDataHashTable.isEmpty()) {
            log.info(String.format("driverLocationValList is null or empty!"));
            return dbValList;
        }

        for (String id : commonPointDataHashTable.keySet()) {
            List<DBVal> DBValList = commonPointDataHashTable.get(id);

            if (DBValList.isEmpty()) {
                continue;
            }

            for (DBVal dbVal : DBValList) {
                DBVal val = DBVal.DBValBuilder.anDBVal()
                        .withTableName(dbVal.getTableName())
                        .withTagName(dbVal.getTagName())
                        .withTagValue(dbVal.getTagValue())
                        .withUtcTime(dbVal.getUtcTimeMilliSeconds())
                        .withValueStatus(dbVal.getValueStatus())
                        .withFieldNames(dbVal.getFieldNames())
                        .withFieldValues(dbVal.getFieldValues())
                        .withFieldTypes(dbVal.getFieldTypes())
                        .withFieldSize(dbVal.getFieldSize())
                        .build();
                dbValList.add(val);
            }
        }

        log.info(String.format("total DBVal size:[%d]", dbValList.size()));
        return dbValList;
    }
}
