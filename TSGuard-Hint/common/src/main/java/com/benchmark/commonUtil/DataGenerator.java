package com.benchmark.commonUtil;

import com.benchmark.constants.DataType;
import com.benchmark.constants.ValueStatus;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PointData;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class DataGenerator {
    private static final String TABLENAME = "driverLocation";
    private static final String TAGNAME = "tagName";
    private static final String FIELDNAME = "value";

    /**
     * @param random
     * @param variance
     * @param mean
     * @return java.lang.Double
     * @description 采取高斯分布生成数据值
     * @dateTime 2023/3/2 8:44
     */
    public static Double generateValue(Random random, double variance, double mean) {
        double value;
        value = Math.sqrt(variance) * random.nextGaussian() + mean;
        return value;
    }

    /**
     * @param pointNamePrefix   点名前缀(tagValue)
     * @param points            点数
     * @param pointBeginIndex   点名起始索引
     * @param dataCountPerPoint 每个点数据量
     * @param beginDate         点起始生成时间
     * @param period            采样周期
     * @param randomSeed        随机种子
     * @param mean              均值
     * @param variance          方差
     * @param debug             是否为调试模式
     * @return java.util.List<dataObject.PointData>
     * @description 按照指定参数，生成若干数目数据点
     * @dateTime 2023/3/2 8:42
     */
    public static List<PointData> generatePointData(String pointNamePrefix, int points, int pointBeginIndex,
                                                    int dataCountPerPoint,
                                                    long beginDate, long period,
                                                    int randomSeed, double mean, double variance, boolean debug) {

        Random random = new Random();
        random.setSeed(randomSeed);
        List<PointData> pointDataList = new ArrayList<>(points * dataCountPerPoint);

        Double value;
        long time;
        for (int i = 0; i < points; i++) {
            for (int j = 0; j < dataCountPerPoint; j++) {
                if (!debug) {
                    value = generateValue(random, variance, mean);
                } else {
                    value = j * 1.0;
                }
                time = beginDate + j * period;

                String pointID = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);

                PointData pointData = PointData.PointDataBuilder.anPointData()
                        .withTableName(TABLENAME)
                        .withTagName(TAGNAME)
                        .withTagValue(pointID)
                        .withUtcTime(time)
                        .withValueStatus(ValueStatus.VALID)
                        .withFieldName(FIELDNAME)
                        .withFieldValue(value)
                        .withFieldType(DataType.DOUBLE)
                        .build();

                pointDataList.add(pointData);
            }
        }
        return pointDataList;
    }


    public static List<PointData> generateDataForAggTest(String pointNamePrefix, int points, int pointBeginIndex,
                                                         int dataCountPerPoint,
                                                         long beginDate, long period,
                                                         int randomSeed, double mean, double variance, boolean debug) {
        Random random = new Random();
        random.setSeed(randomSeed);
        List<PointData> pointDataList = new ArrayList<>(points * dataCountPerPoint);

        Double value;
        Double totolValue = 0.0;
        Double v;
        long time;
        for (int i = 0; i < points; i++) {
            for (int j = 0; j < dataCountPerPoint; j++) {
                if (!debug) {
                    value = generateValue(random, variance, mean);
                } else {
                    value = j * 1.0;
                }
                time = beginDate + j * period;

                Date date = new Date(time);
                totolValue += value;
                v = (double) (date.getHours() + date.getMinutes());

                String pointID = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);

                PointData pointData = PointData.PointDataBuilder.anPointData()
                        .withTableName(TABLENAME)
                        .withTagName(TAGNAME)
                        .withTagValue(pointID)
                        .withUtcTime(time)
                        .withValueStatus(ValueStatus.VALID)
                        .withFieldName(FIELDNAME)
                        .withFieldValue(v)
                        .withFieldType(DataType.DOUBLE)
                        .build();
                pointDataList.add(pointData);
            }
        }
        return pointDataList;
    }


    /**
     * @param pointNamePrefix 点名前缀
     * @param points          点数
     * @param pointBeginIndex 起始索引值
     * @param randomSeed      随机种子
     * @param mean            均值
     * @param variance        方差
     * @param debug           是否为debug模式
     * @return java.util.List<dataObject.PointData>
     * @description 针对当前时间戳，生成若干点，每个点仅含一个数据量
     * @dateTime 2023/3/2 8:57
     */
    public static List<PointData> generateCurrentPointData(String pointNamePrefix, int points, int pointBeginIndex,
                                                           int randomSeed, double mean, double variance, boolean debug) {

        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        random.setSeed(randomSeed);
        List<PointData> pointDataList = new ArrayList<>(points);

        long time = CommonUtil.currentUTCMilliSeconds();
        Double value;
        Double totolValue = 0.0;

        for (int i = 0; i < points; i++) {
            if (!debug)
                value = generateValue(random, variance, mean);
            else {
                value = i + 1.0;
            }

            totolValue += value;
            String pointID = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);

            PointData pointData = PointData.PointDataBuilder.anPointData()
                    .withTableName(TABLENAME)
                    .withTagName(TAGNAME)
                    .withTagValue(pointID)
                    .withUtcTime(time)
                    .withValueStatus(ValueStatus.VALID)
                    .withFieldName(FIELDNAME)
                    .withFieldValue(value)
                    .withFieldType(DataType.DOUBLE)
                    .build();
            pointDataList.add(pointData);
        }
        return pointDataList;
    }

    /**
     * @param pointNamePrefix   点名前缀(tagValue)
     * @param points            点数
     * @param pointBeginIndex   点名起始索引
     * @param dataCountPerPoint 每个点数据量
     * @param beginDate         点起始生成时间
     * @param period            采样周期
     * @param randomSeed        随机种子
     * @param mean              均值
     * @param variance          方差
     * @param debug             是否为调试模式
     * @return java.util.List<dataObject.PointData>
     * @description 按照指定参数，生成若干数目数据点（DBVal）
     * @dateTime 2023/3/2 8:42
     */
    public static List<DBVal> generateDBVal(String pointNamePrefix, int points, int pointBeginIndex,
                                            int dataCountPerPoint,
                                            long beginDate, long period,
                                            int randomSeed, double mean, double variance, boolean debug) {

        Random random = new Random();
        random.setSeed(randomSeed);
        List<DBVal> dbValList = new ArrayList<>(points * dataCountPerPoint);

        Double value;
        long time;
        for (int i = 0; i < points; i++) {
            for (int j = 0; j < dataCountPerPoint; j++) {
                if (!debug) {
                    value = generateValue(random, variance, mean);
                } else {
                    value = j * 1.0;
                }
                time = beginDate + j * period;

                String pointID = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);

                DBVal dbVal = DBVal.DBValBuilder.anDBVal()
                        .withTableName(TABLENAME)
                        .withTagName(TAGNAME)
                        .withTagValue(pointID)
                        .withUtcTime(time)
                        .withValueStatus(ValueStatus.VALID)
                        .build();
                dbVal.addField(FIELDNAME, DataType.DOUBLE, value);
                dbValList.add(dbVal);
            }
        }
        return dbValList;
    }

    /**
     * @param pointNamePrefix 点名前缀
     * @param points          点数
     * @param pointBeginIndex 起始索引值
     * @param randomSeed      随机种子
     * @param mean            均值
     * @param variance        方差
     * @param debug           是否为debug模式
     * @return java.util.List<dataObject.PointData>
     * @description 针对当前时间戳，生成若干点，每个点仅含一个数据量（DBVal）
     * @dateTime 2023/3/2 8:57
     */
    public static List<DBVal> generateCurrentDBVal(String pointNamePrefix, int points, int pointBeginIndex,
                                                   int randomSeed, double mean, double variance, boolean debug) {

        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        random.setSeed(randomSeed);
        List<DBVal> dbValList = new ArrayList<>(points);

        long time = CommonUtil.currentUTCMilliSeconds();
        Double value;
        Double totolValue = 0.0;

        for (int i = 0; i < points; i++) {
            if (!debug)
                value = generateValue(random, variance, mean);
            else {
                value = i + 1.0;
            }

            totolValue += value;
            String pointID = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);

            DBVal dbVal = DBVal.DBValBuilder.anDBVal()
                    .withTableName(TABLENAME)
                    .withTagName(TAGNAME)
                    .withTagValue(pointID)
                    .withUtcTime(time)
                    .withValueStatus(ValueStatus.VALID)
                    .build();
            dbVal.addField(FIELDNAME, DataType.DOUBLE, value);
            dbValList.add(dbVal);
        }
        return dbValList;
    }

    /**
     * @param pointNamePrefix 点名前缀
     * @param points          查询点数
     * @param pointBeginIndex 初始索引值
     * @return java.util.List<com.benchmark.dto.DBValParam>
     * @description 生成查询参数（目前为顺序生成）
     * @dateTime 2023/3/2 14:26
     */
    public static List<DBValParam> generateDBValParams(String pointNamePrefix, int points, int pointBeginIndex) {
        List<DBValParam> results = new ArrayList<>();
        for (int i = 0; i < points; i++) {
            String tagValue = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);
            results.add(DBValParam.builder()
                    .tableName(TABLENAME)
                    .tagName(TAGNAME)
                    .tagValue(tagValue)
                    .fieldName("")
                    .valueStatus(ValueStatus.VALID)
                    .build());
        }
        return results;
    }

    // 生成单个查询点
    public static DBValParam generateDBValParam(String pointNamePrefix, int pointBeginIndex) {
        return DBValParam.builder()
                .tableName(TABLENAME)
                .tagName(TAGNAME)
                .tagValue(String.format("%s%d", pointNamePrefix, pointBeginIndex))
                .fieldName("")
                .valueStatus(ValueStatus.VALID)
                .build();
    }

    // 生成具备FieldName查询实例，暂时用于聚合查询(tsdb不支持全局匹配)
    public static List<DBValParam> generateDBValParamsWithFieldName(String pointNamePrefix, int points, int pointBeginIndex) {
        List<DBValParam> results = new ArrayList<>();
        for (int i = 0; i < points; i++) {
            String tagValue = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);
            results.add(DBValParam.builder()
                    .tableName(TABLENAME)
                    .tagName(TAGNAME)
                    .tagValue(tagValue)
                    .fieldName("value")
                    .valueStatus(ValueStatus.VALID)
                    .build());
        }
        return results;
    }
}
