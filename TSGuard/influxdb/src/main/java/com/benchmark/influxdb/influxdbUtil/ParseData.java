package com.benchmark.influxdb.influxdbUtil;

import com.benchmark.constants.DataType;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.DBVal;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.util.*;

/**
 * 功能描述
 *
 * @author: scott
 * @date: 2022年06月05日 3:54 PM
 */
public class ParseData {
    /*public static Hashtable<String, SortedMap<Long, PointData>> parseDriverLocation(List<FluxTable> tables) {
        //key:String -> tagValue, Long -> time
        Hashtable<String, SortedMap<Long,PointData>> pointDataHashtable = new Hashtable<>();

        String com.benchmark.fieldName;
        String pointName;
        Object pointNameObject;
        SortedMap<Long, PointData> timeSery;
        long timeMilli;
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                pointNameObject =  fluxRecord.getValueByKey(DriverLocationFieldName.DRIVERID.getValue());
                if(pointNameObject==null) {
                    pointName = null;
                }
                else {
                    pointName = pointNameObject.toString();
                }

                timeMilli = fluxRecord.getTime().toEpochMilli();
                PointData pointData;
                if(!pointDataHashtable.containsKey(pointName)) {
                    timeSery = new TreeMap<>();
                    pointDataHashtable.put(pointName, timeSery);
                    pointData = new PointData();
                    pointData.setDriverId(pointName);
                    pointData.setTime(fluxRecord.getTime());
                    timeSery.put(timeMilli, pointData);
                }
                else {
                    timeSery = pointDataHashtable.get(pointName);
                    if(!timeSery.containsKey(timeMilli)) {
                        pointData = new PointData();
                        pointData.setDriverId(pointName);
                        pointData.setTime(fluxRecord.getTime());
                        timeSery.put(timeMilli, pointData);
                    }
                    else {
                        pointData = timeSery.get(timeMilli);
                    }
                }
                com.benchmark.fieldName = fluxRecord.getField();
                pointData.setFieldValue(com.benchmark.fieldName, fluxRecord.getValue());
            }
        }
        return pointDataHashtable;
    }

    *//**
     * 分析tables，tables中只有一个field value
     * @param tables
     * @return
     *//*
    public static Hashtable<String, SortedMap<Long, PointDataWithSingleValue>> parseWithSingleValue(
            List<FluxTable> tables) {
        //key:String -> id, Long -> time
        Hashtable<String, SortedMap<Long,PointDataWithSingleValue>> pointDataHashtable = new Hashtable<>();

        String pointName;
        Object pointNameObject;
        SortedMap<Long, PointDataWithSingleValue> timeSery;
        PointDataWithSingleValue pointData;
        long timeMilli;
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                pointNameObject =  fluxRecord.getValueByKey(DriverLocationFieldName.DRIVERID.getValue());
                if(pointNameObject==null) {
                    pointName = null;
                }
                else {
                    pointName = pointNameObject.toString();
                }

                timeMilli = fluxRecord.getTime().toEpochMilli();
                if(!pointDataHashtable.containsKey(pointName)) {
                    timeSery = new TreeMap<>();
                    pointDataHashtable.put(pointName, timeSery);
                    pointData = new PointDataWithSingleValue();
                    pointData.setPointName(pointName);
                    pointData.setTime(fluxRecord.getTime());
                    timeSery.put(timeMilli, pointData);
                }
                else {
                    timeSery = pointDataHashtable.get(pointName);
                    if(!timeSery.containsKey(timeMilli)) {
                        pointData = new PointDataWithSingleValue();
                        pointData.setPointName(pointName);
                        pointData.setTime(fluxRecord.getTime());
                        timeSery.put(timeMilli, pointData);
                    }
                    else {
                        pointData = timeSery.get(timeMilli);
                    }
                }
                pointData.setValueFromObject(fluxRecord.getValue());
            }
        }
        return pointDataHashtable;
    }

    */

    /**
     * 分析tables，tables中只有一个field value，一个id只有一个value
     *
     * @param
     * @return
     *//*
    public static Hashtable<String, PointDataWithSingleValue> parseIDAndSingleValue(
            List<FluxTable> tables) {
        Hashtable<String, PointDataWithSingleValue> pointDataHashtable = new Hashtable<>();

        String pointName;
        Object pointNameObject;
        PointDataWithSingleValue pointData;
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                pointNameObject =  fluxRecord.getValueByKey(DriverLocationFieldName.DRIVERID.getValue());
                if(pointNameObject==null) {
                    pointName = null;
                }
                else {
                    pointName = pointNameObject.toString();
                }

                pointData = new PointDataWithSingleValue();
                pointData.setPointName(pointName);
                pointData.setTime(fluxRecord.getTime());
                pointData.setValueFromObject(fluxRecord.getValue());
                pointDataHashtable.put(pointName, pointData);
            }
        }
        return pointDataHashtable;
    }*/
    public static int localSecondToUtc(int second) {
        int result = second - 8 * 3600;
        if (result < 0)
            result += 24 * 3600;
        return result;
    }

    public static List<String> generateContinuesIDs(String idPrefix, int beginIndex, int count) {
        List<String> result = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            result.add(idPrefix + (beginIndex + i));
        }
        return result;
    }

    /*public static int recordCount(Hashtable<String, SortedMap<Long, PointData>> mapMap) {
        int count = 0;
        for (Object key : mapMap.keySet()) {
            count += mapMap.get(key).size();
        }
        return count;
    }*/

    public static String timeInSecondsRangeFilterString(int beginSecondInADay, int endSecondInADay) {
        StringBuffer sb = new StringBuffer();
        String timeSeconds = "((date.hour(t:r._time)+8)%24)*3600+date.minute(t:r._time)*60+date.second(t:r._time)";
        sb.append("(");
        sb.append(String.format("(%s)>=%d", timeSeconds, beginSecondInADay));
        sb.append(" and ");
        sb.append(String.format("(%s)<=%d", timeSeconds, endSecondInADay));
        sb.append(")");
        return sb.toString();
    }


    //  *************************************************************************************************

    /**
     * @param tables
     * @param tagName
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, db.DBVal>>
     * @description 读取FluxTable对象，将其转为Hashtable形式，key: tagValue; Long: timestamp; DBVal
     * @dateTime 2023/2/15 20:26
     */
    public static Hashtable<String, SortedMap<Long, DBVal>> parseFluxTableToDBVal(List<FluxTable> tables,
                                                                                  String tagName) {
        //key:String -> tagValue, Long -> time
        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = new Hashtable<>();
        if (tables.isEmpty()) {
            return dbValHashtable;
        }

        String tagValue;
        Object tagValueObject;
        SortedMap<Long, DBVal> timeSery;
        long timeMilli;
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                tagValueObject = fluxRecord.getValueByKey(tagName);
                if (tagValueObject == null) {
                    // tagValue = null;
                    continue;
                } else {
                    tagValue = tagValueObject.toString();
                }

                timeMilli = fluxRecord.getTime().toEpochMilli();
                DBVal dbVal;
                if (!dbValHashtable.containsKey(tagValue)) {
                    timeSery = new TreeMap<>();
                    dbValHashtable.put(tagValue, timeSery);
                    dbVal = DBVal.DBValBuilder.anDBVal()
                            .withTableName(fluxRecord.getMeasurement())
                            .withTagName(tagName)
                            .withTagValue(tagValue)
                            .withUtcTime(timeMilli)
                            .build();
                    timeSery.put(timeMilli, dbVal);
                } else {
                    timeSery = dbValHashtable.get(tagValue);
                    if (!timeSery.containsKey(timeMilli)) {
                        dbVal = DBVal.DBValBuilder.anDBVal()
                                .withTableName(fluxRecord.getMeasurement())
                                .withTagName(tagName)
                                .withTagValue(tagValue)
                                .withUtcTime(timeMilli)
                                .build();
                        timeSery.put(timeMilli, dbVal);
                    } else {
                        dbVal = timeSery.get(timeMilli);
                    }
                }
                String fieldName = fluxRecord.getField();
                // TODO 目前仅支持double类型数据获取，后续通过引入commonFieldName对象确定导出数据类型
                double fieldValue = 0.0;
                if (fluxRecord.getValue() != null) {
                    fieldValue = Double.parseDouble(fluxRecord.getValue().toString());
                } else {
                    // TODO 抛出取值异常
                }
                dbVal.addField(fieldName, DataType.DOUBLE, fieldValue);
            }
        }
        return dbValHashtable;
    }

    /**
     * @param dbValParamList
     * @return java.lang.String
     * @description 将若干DBValParams转为查询过滤语句【针对某批次tagName相同的对象】
     * @dateTime 2023/3/1 14:32
     */
    public static String dbValParamsToFilterString(String tagName, List<DBValParam> dbValParamList) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean[] allTagValue = {false};

        dbValParamList.forEach(dbValParam -> {
            if (dbValParam.getTagValue().isEmpty() || dbValParam.getTagValue().equals("*"))
                allTagValue[0] = true;
            stringBuilder.append("r.").append(tagName).append(" == \"")
                    .append(dbValParam.getTagValue()).append("\" or ");
        });

        if (allTagValue[0]) {
            return String.format("r.%s =~ /[\\s\\S*]/", tagName);
        }

        return stringBuilder.substring(0, stringBuilder.length() - 4);
    }

    /**
     * @param val
     * @return java.lang.String
     * @description 将DBVal转为待发送行协议格式，sendPointDatas调用
     * @dateTime 2023/2/4 16:44
     */
    public static String convertToLineProtocol(DBVal val) {
        return val.toString();
    }
}

