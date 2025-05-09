package com.benchmark.iotdb.iotdbUtil;

import com.benchmark.commonUtil.CommonUtil;
import com.benchmark.iotdb.db.Record;
import com.benchmark.iotdb.db.TSVal;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class DataGenerator {

    public static Double generateValue(Random random, double variance, double mean) {
        double value;
        value = Math.sqrt(variance) * random.nextGaussian() + mean;
        return value;
    }

    /**
     * @param storagePath       measurementName + tags
     * @param pointNamePrefix   fields
     * @param points
     * @param pointBeginIndex
     * @param dataCountPerPoint
     * @param beginDate
     * @param period
     * @param randomSeed
     * @param mean
     * @param variance
     * @param debug
     * @return java.util.List<db.PointData>
     * @description 随机生成一系列DBVal数据点
     * @dateTime 2022/12/1 13:07
     */
    public static List<Record> generateData(String storagePath, String pointNamePrefix, int points,
                                            int pointBeginIndex, int dataCountPerPoint,
                                            long beginDate, long period,
                                            int randomSeed, double mean, double variance, boolean debug) {
        Random random = new Random();
        random.setSeed(randomSeed);
        List<Record> recordList = new ArrayList<>(points * dataCountPerPoint);

        // 时间戳
        for (int j = 0; j < dataCountPerPoint; j++) {
            List<String> measurements = new ArrayList<>();
            List<TSDataType> types = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            long time = beginDate + j * period;
            Record record = new Record();
            Double value;

            // 产生多个field
            for (int i = 0; i < points; i++) {
                if (!debug) {
                    value = generateValue(random, variance, mean);
                } else {
                    value = j * 1.0;
                }

                String measurement = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);
                TSDataType type = TSDataType.DOUBLE;

                measurements.add(measurement);
                types.add(type);
                values.add(value);
            }

            record.setPrefixPath(storagePath);
            record.setTimestamp(time);

            record.setMeasurements(measurements);
            record.setTypes(types);
            record.setValues(values);

            recordList.add(record);
        }

        return recordList;
    }

    /**
     * @param storagePath     measurementName + tags
     * @param pointNamePrefix fields_index
     * @param points          生成field数量
     * @param pointBeginIndex
     * @param randomSeed
     * @param mean
     * @param variance
     * @param debug
     * @return db.Record
     * @description 根据当前时间戳生成多条point数据
     * @dateTime 2022/12/1 17:25
     */
    public static Record generateCurrentData(String storagePath, String pointNamePrefix,
                                             int points, int pointBeginIndex, int randomSeed,
                                             double mean, double variance, boolean debug) {
        Random random = new Random();
        random.setSeed(randomSeed);

        Record record = new Record();
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        long time = CommonUtil.currentUTCMilliSeconds();
        Double value;
        Double totolValue = 0.0;

        for (int i = 0; i < points; i++) {
            if (!debug)
                value = Math.sqrt(variance) * random.nextGaussian() + mean;
            else {
                value = i + 1.0;
            }

            totolValue += value;

            String measurement = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);
            measurements.add(measurement);
            types.add(TSDataType.DOUBLE);
            values.add(value);
        }

        record.setMeasurements(measurements);
        record.setValues(values);
        record.setTypes(types);
        record.setTimestamp(time);
        record.setPrefixPath(storagePath);

        return record;
    }

    /**
     * @return java.util.List<db.TSVal>
     * @description 随机生成一系列时间序列
     * @dateTime 2022/11/29 19:14
     */
    public static List<TSVal> generateTimeseries() {
        List<TSVal> timeseriesList = new ArrayList<>();
        TSVal example = new TSVal("root.ln3.wf01.wt02.voltage", TSDataType.INT64,
                TSEncoding.PLAIN, CompressionType.GZIP);
        TSVal example2 = new TSVal("root.ln3.wf01.wt02.pressure", TSDataType.FLOAT,
                TSEncoding.GORILLA, CompressionType.GZIP);

        timeseriesList.add(example);
        timeseriesList.add(example2);
        return timeseriesList;
    }

    public static List<TSVal> generateTimeseries(String storagePath, String pointNamePrefix, int points,
                                                 int pointBeginIndex) {
        List<TSVal> timeseriesList = new ArrayList<>();

        // 按照给定point产生多个时间序列
        for (int i = 0; i < points; i++) {
            String measurement = String.format("%s%d", pointNamePrefix, pointBeginIndex + i);
            String timeseriesName = String.format("%s.%s", storagePath, measurement);
            TSVal timeseries = new TSVal(timeseriesName, TSDataType.DOUBLE,
                    TSEncoding.GORILLA, CompressionType.GZIP);
            timeseriesList.add(timeseries);
        }

        return timeseriesList;
    }

    /**
     * @return org.apache.iotdb.tsfile.write.record.Tablet
     * @description 随机生成Tablet
     * @dateTime 2022/11/29 20:45
     */
    public static Tablet generateTablet() {
        String deviceId = "root.ln.wf01.wt01";
        List<MeasurementSchema> schema = new ArrayList<>();
        //measurementId可乱序，相当于set集合，表明某个度量值具体的value属性
        MeasurementSchema measurementschema = new MeasurementSchema("pressure",
                TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.GZIP);
        MeasurementSchema measurementschema2 = new MeasurementSchema("voltage",
                TSDataType.INT64, TSEncoding.PLAIN, CompressionType.GZIP);

        schema.add(measurementschema);
        schema.add(measurementschema2);

        Tablet tablet = new Tablet(deviceId, schema);

        if (tablet == null) System.out.println("null");

        //可同时为多个度量插入若干各dataPoint(支持留空白，会自添空值)，添加value指明度量名即可
        int pressureRowSize = 2;            //pressure dataPoint数目
        long timestamp = new Date().getTime();
        for (int i = 0; i < pressureRowSize; i++) {
            tablet.addTimestamp(i, timestamp + i);
            tablet.addValue("pressure", i, (float) 5.22);
        }

        int voltageRowSize = 4;            //voltage dataPoint数目
        for (int i = 0; i < voltageRowSize; i++) {
            tablet.addTimestamp(i, timestamp + i);
            tablet.addValue("voltage", i, (long) 10);
        }

        //Bitmap
        tablet.initBitMaps();

        tablet.rowSize += pressureRowSize;
        tablet.rowSize += voltageRowSize;
        if (tablet.rowSize == 0) System.out.println("zero");

        return tablet;
    }

    /**
     * @return db.Record
     * @description 随机生成Record - Record
     * @dateTime 2022/11/29 23:25
     */
    public static Record generatePointData() {
        String prefixPath = "root.ln2.wf01.wt01";        //设备ID
        List<String> measurements = new ArrayList<>();  //fields
        measurements.add("pressure");
        measurements.add("voltage");
        List<TSDataType> types = new ArrayList<>();     //types
        types.add(TSDataType.FLOAT);
        types.add(TSDataType.INT64);
        List<Object> values = new ArrayList<>();        //values
        values.add((float) 10.2);
        values.add((long) 66);
        long timestamp = new Date().getTime();          //timestamp

        Record pointdate = new Record(prefixPath, measurements, types, values, timestamp);
        return pointdate;
    }

    /**
     * @return java.util.List<db.Record>
     * @description 随机生成多条Record - Record，临时测试，后期将采取多次调用生成单条数据生成样本
     * @dateTime 2022/11/30 9:14
     */
    public static List<Record> generateMultiPointData() {
        String prefixPath = "root.ln3.wf01.wt01";        //设备ID
        List<String> measurements = new ArrayList<>();  //fields
        measurements.add("pressure2");
        measurements.add("voltage2");
        List<TSDataType> types = new ArrayList<>();     //types
        types.add(TSDataType.FLOAT);
        types.add(TSDataType.INT64);
        List<Object> values = new ArrayList<>();        //values
        values.add((float) 10.2);
        values.add((long) 66);
        long timestamp = new Date().getTime();          //timestamp

        String prefixPath2 = "root.ln3.wf01.wt02";        //设备ID2
        List<String> measurements2 = new ArrayList<>();  //fields
        measurements2.add("pressure3");
        measurements2.add("voltage3");
        List<TSDataType> types2 = new ArrayList<>();     //types
        types2.add(TSDataType.FLOAT);
        types2.add(TSDataType.INT64);
        List<Object> values2 = new ArrayList<>();        //values
        values2.add((float) 10.2);
        values2.add((long) 66);
        long timestamp2 = new Date().getTime();          //timestamp

        Record pointdate = new Record(prefixPath, measurements, types, values, timestamp);
        Record pointdate2 = new Record(prefixPath2, measurements2, types2, values2, timestamp2);

        List<Record> pointdata = new ArrayList<>();
        pointdata.add(pointdate);
        pointdata.add(pointdate2);

        return pointdata;
    }
}
