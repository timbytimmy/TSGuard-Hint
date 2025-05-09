package com.benchmark.iotdb.iotdbUtil;

import com.benchmark.constants.DataType;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.DBVal;
import com.benchmark.iotdb.db.PointData;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.*;
import java.util.logging.Logger;

public class ParseUtil {
    public static final Logger logger = Logger.getLogger(ParseUtil.class.getName());

    /**
     * @param sessionDataSet iotdb查询返回结果
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, db.PointData>>
     * @description 将查询返回的数据结构转换为HashTable<field, < time, value>>
     * @dateTime 2022/11/30 15:32
     */
    public static Hashtable<String, SortedMap<Long, PointData>> parseForQueryLast(SessionDataSet sessionDataSet) {
        Hashtable<String, SortedMap<Long, PointData>> pointDataHashtable = new Hashtable<>();
        String path;
        TSDataType dataType;
        Object value;
        Long timeMilli;
        SortedMap<Long, PointData> timeSery;
        try {
            //解析sessionDataSet对象为Hashtable
            while (sessionDataSet.hasNext()) {
                RowRecord record = sessionDataSet.next();

                // 注意：该List种，三种数据的位置可能取决于iotdb官方放置数据的位置，后续需改进
                List<Field> fieldList = record.getFields();

                path = fieldList.get(0).toString();
                dataType = typeNameToTSDataType(fieldList.get(2).toString());
                value = fieldList.get(1);

                timeMilli = record.getTimestamp();

                String[] paths = path.split("\\.");

                PointData pointData;
                pointData = PointData.builder()
                        .tableName(paths[1])
                        .tagValue(paths[2])
                        .fieldName(paths[3])
                        .fieldValue(value)
                        .fieldType(dataType)
                        .utcTime(timeMilli)
                        .build();

                if (!pointDataHashtable.containsKey(path)) {
                    timeSery = new TreeMap<>();
                    timeSery.put(timeMilli, pointData);
                    pointDataHashtable.put(path, timeSery);
                } else {
                    timeSery = pointDataHashtable.get(path);
                    if (!timeSery.containsKey(timeMilli)) {
                        timeSery.put(timeMilli, pointData);
                    }
                }
            }
        } catch (Exception e) {
            logger.warning(String.format("解析结果异常: %s", e));
        }
        return pointDataHashtable;
    }

    /**
     * @param sessionDataSet
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, db.PointData>>
     * @description 为queryTimeRange函数解析结果集
     * @dateTime 2022/11/30 20:25
     */
    public static Hashtable<String, SortedMap<Long, PointData>> parseForQueryTimeRange(SessionDataSet sessionDataSet) {
        Hashtable<String, SortedMap<Long, PointData>> pointDataHashtable = new Hashtable<>();
        String path;
        TSDataType dataType;
        Object value;
        Long timeMilli;
        SortedMap<Long, PointData> timeSery;
        // 列名放在columnNameList，0为Time，其余为列对象
        try {
            // columnNameList: 0存储Time，从1开始存储若干record的path
            List<String> columnNameList = sessionDataSet.getColumnNames();

            //解析sessionDataSet对象为Hashtable
            while (sessionDataSet.hasNext()) {
                RowRecord record = sessionDataSet.next();
                List<Field> fieldList = record.getFields();
                timeMilli = record.getTimestamp();

                int cnt = 1;
                for (Field field : fieldList) {
                    dataType = field.getDataType();
                    path = columnNameList.get(cnt++);
                    value = field.getObjectValue(dataType);

                    PointData pointData = PointData.builder().build();
                    // point为null
                    if (dataType == null) {
                        pointData.setValueStatusInvalid();
                    } else {
                        // 切割path，目前仅支持单tag模式，后续扩展。组成结构: root+measurement+tagValues+com.benchmark.fieldName
                        String[] paths = path.split("\\.");

                        pointData.setTableName(paths[1]);
                        pointData.setTagValue(paths[2]);
                        pointData.setFieldName(paths[3]);
                        pointData.setFieldValue(value);
                        pointData.setFieldType(dataType);
                        pointData.setUtcTimeMilliSeconds(timeMilli);

                        if (!pointDataHashtable.containsKey(path)) {
                            timeSery = new TreeMap<>();
                            pointDataHashtable.put(path, timeSery);
                            timeSery.put(timeMilli, pointData);
                        } else {
                            timeSery = pointDataHashtable.get(path);
                            if (!timeSery.containsKey(timeMilli)) {
                                timeSery.put(timeMilli, pointData);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warning(String.format("解析结果异常: %s", e));
        }
        return pointDataHashtable;
    }

    public static TSDataType stringTransTSDataType(String dataType) {
        return TSDataType.FLOAT;
    }

    // TODO TagName于数据库内无标记

    /**
     * @param dbValParam
     * @return java.util.List<java.lang.String>
     * @description 将dbVal的tableName、tagName等转换为存储路径[目前命名规则：root.tableName.tagValues]
     * @dateTime 2023/2/16 18:00
     */
    public static String parseDBValParamToPath(DBValParam dbValParam) {
        String tableName = dbValParam.getTableName();
        String tagValue = dbValParam.getTagValue();

        if (tableName.isEmpty()) tableName = "*";
        if (tagValue.isEmpty()) tagValue = "*";

        // TODO 当支持多个tags，针对某一tag搜索，需要更改路径转换规则
        return String.format("root.%s.%s.*", tableName, tagValue);
    }

    /**
     * @param dbValParams
     * @return java.util.List<java.lang.String>
     * @description 将多个dbVal的tableName、tagName（多个解析对象tableName、tagName一致）等转换为存储路径[目前命名规则：root.tableName.tagValues]
     * @dateTime 2023/2/16 18:00
     */
    public static List<String> parseDBValParamsToPaths(String tableName, List<DBValParam> dbValParams) {
        List<String> results = new ArrayList<>();

        // 使用set集合去重
        Set<String> paths = new LinkedHashSet<>();
        boolean[] allPath = {false, false};       // allTableName、allTagValue

        dbValParams.forEach(dbValParam -> {
            if (dbValParam.getTableName().isEmpty() || dbValParam.getTableName().equals("*")) {
                allPath[0] = true;
            }
            if (dbValParam.getTagValue().isEmpty() || dbValParam.getTagValue().equals("*")) {
                allPath[1] = true;
            }
            paths.add(String.format("root.%s.%s.*", dbValParam.getTableName(), dbValParam.getTagValue()));
        });

        if (allPath[1]) {
            paths.clear();
            paths.add(String.format("root.%s.*.*", tableName));
        }
        if (allPath[0]) {
            paths.clear();
            // tableName、tagValue、com.benchmark.fieldName
            paths.add("root.*.*.*");
        }

        paths.iterator().forEachRemaining(results::add);
        return results;
    }

    /**
     * @param sessionDataSet iotdb查询返回结果
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, db.PointData>>
     * @description 将查询返回的数据结构转换为HashTable<tagValue, < time, dbval>>
     * @dateTime 2022/11/30 15:32
     */
    public static Hashtable<String, SortedMap<Long, DBVal>> parseResultToDBValsLast(String tagName,
                                                                                    SessionDataSet sessionDataSet) {
        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = new Hashtable<>();

        try {
            //解析sessionDataSet对象为Hashtable
            while (sessionDataSet.hasNext()) {
                RowRecord record = sessionDataSet.next();

                // 注意：List内部，0为存储路径，1为fieldValue值，2为fieldType
                List<Field> fieldList = record.getFields();

                String path = fieldList.get(0).toString();
                TSDataType fieldType = typeNameToTSDataType(fieldList.get(2).toString());
                Object fieldValue = fieldList.get(1);
                long timeMilli = record.getTimestamp();

                // TODO 当支持若干tag时，从第二个开始，直到倒数第一个均为tagValue
                String[] paths = path.split("\\.");

                String tableName = paths[1];
                String tagValue = paths[2];
                String fieldName = paths[paths.length - 1];
                if (tagValue.isEmpty()) {
                    continue;
                }

                // 获取DBVal对象
                DBVal dbVal;

                if (dbValHashtable.containsKey(tagValue)) {
                    SortedMap<Long, DBVal> timeSery = dbValHashtable.get(tagValue);
                    if (timeSery.containsKey(timeMilli)) {
                        dbVal = timeSery.get(timeMilli);
                    } else {
                        dbVal = DBVal.DBValBuilder.anDBVal()
                                .withTableName(tableName)
                                .withTagName(tagName)
                                .withTagValue(tagValue)
                                .withUtcTime(timeMilli)
                                .build();
                        timeSery.put(timeMilli, dbVal);
                    }
                } else {
                    dbVal = DBVal.DBValBuilder.anDBVal()
                            .withTableName(tableName)
                            .withTagName(tagName)
                            .withTagValue(tagValue)
                            .withUtcTime(timeMilli)
                            .build();
                    SortedMap<Long, DBVal> timeSery = new TreeMap<>();
                    timeSery.put(timeMilli, dbVal);
                    dbValHashtable.put(tagValue, timeSery);
                }
                dbVal.addField(fieldName, DataType.getDataType(fieldType.serialize()), fieldValue);
            }
        } catch (Exception e) {
            logger.warning(String.format("解析结果异常: %s", e));
        }
        return dbValHashtable;
    }

    /**
     * @param tagName
     * @param sessionDataSet
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, dataObject.DBVal>>
     * @description 解析按照时间范围查询数据返回的结果
     * @dateTime 2023/2/16 20:28
     */
    public static Hashtable<String, SortedMap<Long, DBVal>> parseResultToDBValsTimeRange(String tagName,
                                                                                         SessionDataSet sessionDataSet) {
        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = new Hashtable<>();

        // 列名放在columnNameList，0为Time，其余为列对象
        try {
            // columnNameList: 0存储Time，从1开始存储若干record的path
            List<String> columnNameList = sessionDataSet.getColumnNames();

            //解析sessionDataSet对象为Hashtable
            while (sessionDataSet.hasNext()) {
                RowRecord record = sessionDataSet.next();
                List<Field> fieldList = record.getFields();
                long timeMilli = record.getTimestamp();

                int cnt = 1;
                for (Field field : fieldList) {
                    TSDataType fieldType = field.getDataType();
                    String path = columnNameList.get(cnt++);
                    Object fieldValue = field.getObjectValue(fieldType);

                    // point为null
                    if (fieldType == null) {
                        continue;
                    }

                    // TODO 由于插入路径问题，若支持多个tags，下列索引需要根据tag数目进行修改
                    String[] paths = path.split("\\.");
                    String tableName = paths[1];
                    String tagValue = paths[2];
                    String fieldName = paths[paths.length - 1];

                    if (tagValue.isEmpty()) {
                        continue;
                    }

                    // 获取DBVal对象
                    DBVal dbVal;

                    if (dbValHashtable.containsKey(tagValue)) {
                        SortedMap<Long, DBVal> timeSery = dbValHashtable.get(tagValue);
                        if (timeSery.containsKey(timeMilli)) {
                            dbVal = timeSery.get(timeMilli);
                        } else {
                            dbVal = DBVal.DBValBuilder.anDBVal()
                                    .withTableName(tableName)
                                    .withTagName(tagName)
                                    .withTagValue(tagValue)
                                    .withUtcTime(timeMilli)
                                    .build();
                            timeSery.put(timeMilli, dbVal);
                        }
                    } else {
                        dbVal = DBVal.DBValBuilder.anDBVal()
                                .withTableName(tableName)
                                .withTagName(tagName)
                                .withTagValue(tagValue)
                                .withUtcTime(timeMilli)
                                .build();
                        SortedMap<Long, DBVal> timeSery = new TreeMap<>();
                        timeSery.put(timeMilli, dbVal);
                        dbValHashtable.put(tagValue, timeSery);
                    }
                    dbVal.addField(fieldName, DataType.getDataType(fieldType.serialize()), fieldValue);
                }
            }
        } catch (Exception e) {
            logger.warning(String.format("解析结果异常: %s", e));
        }

        return dbValHashtable;
    }

    /**
     * @param tagName
     * @param sessionDataSet
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, com.benchmark.entity.DBVal>>
     * @description 解析聚合、降采样数据结果
     * @dateTime 2023/3/19 17:07
     */
    public static Hashtable<String, SortedMap<Long, DBVal>> parseAggResultToDBVal(String tagName,
                                                                                  SessionDataSet sessionDataSet) {
        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = new Hashtable<>();

        // 列名放在columnNameList，0为Time，其余为列对象
        try {
            // columnNameList: 0存储Time，从1开始存储若干record的path
            List<String> columnNameList = sessionDataSet.getColumnNames();

            //解析sessionDataSet对象为Hashtable
            while (sessionDataSet.hasNext()) {
                RowRecord record = sessionDataSet.next();
                List<Field> fieldList = record.getFields();
                long timeMilli = record.getTimestamp();

                int cnt = 0;
                for (Field field : fieldList) {
                    TSDataType fieldType = field.getDataType();
                    String path = columnNameList.get(cnt++);
                    path = path.substring(0, path.length() - 1);        // 去除尾部括号
                    Object fieldValue = field.getObjectValue(fieldType);

                    // point为null
                    if (fieldType == null) {
                        continue;
                    }

                    // TODO 由于插入路径问题，若支持多个tags，下列索引需要根据tag数目进行修改
                    String[] paths = path.split("\\.");
                    String tableName = paths[1];
                    String tagValue = paths[2];
                    String fieldName = paths[paths.length - 1];

                    if (tagValue.isEmpty()) {
                        continue;
                    }

                    // 获取DBVal对象
                    DBVal dbVal;

                    if (dbValHashtable.containsKey(tagValue)) {
                        SortedMap<Long, DBVal> timeSery = dbValHashtable.get(tagValue);
                        if (timeSery.containsKey(timeMilli)) {
                            dbVal = timeSery.get(timeMilli);
                        } else {
                            dbVal = DBVal.DBValBuilder.anDBVal()
                                    .withTableName(tableName)
                                    .withTagName(tagName)
                                    .withTagValue(tagValue)
                                    .withUtcTime(timeMilli)
                                    .build();
                            timeSery.put(timeMilli, dbVal);
                        }
                    } else {
                        dbVal = DBVal.DBValBuilder.anDBVal()
                                .withTableName(tableName)
                                .withTagName(tagName)
                                .withTagValue(tagValue)
                                .withUtcTime(timeMilli)
                                .build();
                        SortedMap<Long, DBVal> timeSery = new TreeMap<>();
                        timeSery.put(timeMilli, dbVal);
                        dbValHashtable.put(tagValue, timeSery);
                    }
                    dbVal.addField(fieldName, DataType.getDataType(fieldType.serialize()), fieldValue);
                }
            }
        } catch (Exception e) {
            logger.warning(String.format("解析结果异常: %s", e));
        }

        return dbValHashtable;
    }

    /**
     * @param typeName
     * @return org.apache.iotdb.tsfile.file.metadata.enums.TSDataType
     * @description 根据typeName将类型转为TSDataType
     * @dateTime 2023/2/16 20:20
     */
    public static TSDataType typeNameToTSDataType(String typeName) {
        if (typeName.equals("BOOLEAN")) {
            return TSDataType.BOOLEAN;
        } else if (typeName.equals("INT32")) {
            return TSDataType.INT32;
        } else if (typeName.equals("INT64")) {
            return TSDataType.INT64;
        } else if (typeName.equals("FLOAT")) {
            return TSDataType.FLOAT;
        } else if (typeName.equals("DOUBLE")) {
            return TSDataType.DOUBLE;
        } else if (typeName.equals("TEXT")) {
            return TSDataType.TEXT;
        } else if (typeName.equals("VECTOR")) {
            return TSDataType.VECTOR;
        } else {
            logger.warning("解析类型名错误,未知的类型传参");
            // TODO 抛异常
            return null;
        }
    }
}
