package com.benchmark.iotdb.iotdbUtil;

import com.benchmark.commonUtil.SpentTimeCalculator;
import com.benchmark.constants.DataType;
import com.benchmark.entity.DBVal;
import com.benchmark.iotdb.Globals;
import com.benchmark.iotdb.db.IotdbDBApiEntry;
import lombok.NonNull;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.*;
import java.util.logging.Logger;

public class CommonPointDataReader {
    public static final Logger logger = Logger.getLogger(CommonPointDataReader.class.getName());
    private static String charSet = "utf-8";

    public static Map<String, Tablet> commonPointDataToTablet(
            Hashtable<String, List<DBVal>> commonPointDataHashtable) {
        Map<String, Tablet> tablets = new HashMap<>();
        if (commonPointDataHashtable.isEmpty()) {
            logger.info(String.format("driverLocationValHashtable is empty!"));
            return tablets;
        }

        long totalTabletSize = 0L;
        // 遍历每一台driverId，为其声明tablet，相当于以汽车id命名的表
        for (String id : commonPointDataHashtable.keySet()) {
            List<DBVal> DBValList = commonPointDataHashtable.get(id);

            if (DBValList.isEmpty()) {
                continue;
            }

            // 根据driverId命名设备id，并充当度量名
            //TODO 目前仅支持单个tag，后续扩展多tags，增加路径长度即可（目前路径命名：root+measurement+tags）
            String deviceId = String.format("root." + DBValList.get(0).getTableName() + "." + id);
            List<MeasurementSchema> schema = new ArrayList<>();
            {
                // measurementId可乱序，相当于set集合，表明某个field值具体的value属性;一台设备下的measurement应该保持一致
                List<String> fieldNames = DBValList.get(0).getFieldNames();
                List<DataType> fieldTypes = DBValList.get(0).getFieldTypes();
                for (int i = 0; i < DBValList.get(0).getFieldSize(); i++) {
                    String fieldName = fieldNames.get(i);
                    DataType fieldType = fieldTypes.get(i);

                    MeasurementSchema measurementschema = new MeasurementSchema(fieldName, dataTypeToTSDataType(fieldType),
                            getTSEncodingByDataType(fieldType), getCompressionTypeByDataType(fieldType));

                    schema.add(measurementschema);
                }
            }
            // 声明tablet：指定单个tablet最大行数为10000，默认1024
            Tablet tablet = new Tablet(deviceId, schema, 30000);

            // 针对单独tablet添加value值
            // 可同时为多个度量插入若干各dataPoint(支持留空白，会自添空值)，添加value指明度量名即可
            int index = 0;
            for (DBVal DBVal : DBValList) {
                tablet.addTimestamp(index, DBVal.getUtcTimeMilliSeconds());

                List<String> fieldNames = DBVal.getFieldNames();
                List<DataType> fieldTypes = DBVal.getFieldTypes();
                List<Object> fieldValues = DBVal.getFieldValues();

                for (int i = 0; i < DBValList.get(0).getFieldSize(); i++) {
                    String fieldName = fieldNames.get(i);
                    DataType fieldType = fieldTypes.get(i);
                    Object fieldValue = fieldValues.get(i);

                    //TODO 获取解析的fieldValue值
                    if (fieldType == DataType.BOOLEAN) {
                        boolean booleanValue = Boolean.parseBoolean(fieldValue.toString());
                        tablet.addValue(fieldName, index, booleanValue);
                    } else if (fieldType == DataType.INT32) {
                        // TODO 若JSON文件带小数点的字符串，无法解析为INT或LONG，目前暂将其通过Double接收，强转int; 需修改
                        double doubleValue = Double.parseDouble(fieldValue.toString());
                        int intValue = (int) doubleValue;
                        tablet.addValue(fieldName, index, intValue);
                    } else if (fieldType == DataType.INT64) {
                        long longValue = Long.parseLong(fieldValue.toString());
                        tablet.addValue(fieldName, index, longValue);
                    } else if (fieldType == DataType.FLOAT) {
                        float floatValue = Float.parseFloat(fieldValue.toString());
                        tablet.addValue(fieldName, index, floatValue);
                    } else if (fieldType == DataType.DOUBLE) {
                        double doubleValue = Double.parseDouble(fieldValue.toString());
                        tablet.addValue(fieldName, index, doubleValue);
                    } else if (fieldType == DataType.TEXT) {
                        String stringValue = fieldValue.toString();
                        tablet.addValue(fieldName, index, stringValue);
                    } else {
                        //TODO 抛出解析异常
                    }
                }

                index++;
            }
            // Bitmap
            tablet.initBitMaps();
            tablet.rowSize += DBValList.size();
            totalTabletSize += DBValList.size();

            // 将tablet添加进Map
            tablets.put(deviceId, tablet);
        }
        logger.info(String.format("all tablet size is:[%d]", totalTabletSize));
        return tablets;
    }

    /**
     * @param dataType
     * @return org.apache.iotdb.tsfile.file.metadata.enums.TSDataType
     * @description 将自定义DataType转为TSDataType
     * @dateTime 2023/2/3 17:35
     */
    public static TSDataType dataTypeToTSDataType(DataType dataType) {
        return TSDataType.getTsDataType(dataType.getType());
    }

    /**
     * @return org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding
     * @description 根据自定义DataType获取自定义Encoding方案，可自定义
     * @dateTime 2023/2/3 17:37
     */
    @NonNull
    public static TSEncoding getTSEncodingByDataType(DataType dataType) {
        // RLE: bool、int32
        // GORILLA: int64、float、double
        // PLAIN: text
        switch (dataType.getType()) {
            case 0:
            case 1:
                return TSEncoding.RLE;
            case 2:
            case 3:
            case 4:
                return TSEncoding.GORILLA;
            case 5:
                return TSEncoding.PLAIN;
            case 6:
            default:
                //TODO 抛出异常
                // throw new IllegalArgumentException("Invalid input: " + type);
                return TSEncoding.PLAIN;
        }
    }

    /**
     * @return org.apache.iotdb.tsfile.file.metadata.enums.CompressionType
     * @description 根据DataType获取指定Compression方案，目前所有数据类型均采用SNAPPY压缩（针对二进制，适用于各种数据类型）
     * @dateTime 2023/2/3 17:39
     */
    public static CompressionType getCompressionTypeByDataType(DataType dataType) {
        return CompressionType.SNAPPY;
    }

    /**
     * @param commonPointDataHashtable
     * @param chunkNum
     * @param host
     * @return void
     * @description 将driverLocationValHashtable分批插入iotdb数据库
     * @dateTime 2023/2/2 10:51
     */
    public static void sendTabletsInbatchs(
            Hashtable<String, List<DBVal>> commonPointDataHashtable,
            int chunkNum, String host) {
        if (commonPointDataHashtable == null) {
            logger.info(String.format("driverLocationValHashtable is null or empty!"));
            return;
        }

        Map<String, Tablet> tablets = new HashMap<>();
        long totalTabletSize = 0L;
        // 遍历每一台driverId，为其声明tablet，相当于以汽车id命名的表
        for (String id : commonPointDataHashtable.keySet()) {
            List<DBVal> DBValList = commonPointDataHashtable.get(id);

            if (DBValList == null || DBValList.isEmpty()) {
                continue;
            }

            // 根据driverId命名设备id，并充当度量名
            //TODO 目前仅支持单个tag，后续扩展多tags，增加路径长度即可（目前路径命名：root+measurement+tags）
            String deviceId = String.format("root." + DBValList.get(0).getTableName() + "." + id);
            List<MeasurementSchema> schema = new ArrayList<>();
            {
                // measurementId可乱序，相当于set集合，表明某个field值具体的value属性;一台设备下的measurement应该保持一致
                List<String> fieldNames = DBValList.get(0).getFieldNames();
                List<DataType> fieldTypes = DBValList.get(0).getFieldTypes();
                for (int i = 0; i < DBValList.get(0).getFieldSize(); i++) {
                    String fieldName = fieldNames.get(i);
                    DataType fieldType = fieldTypes.get(i);

                    MeasurementSchema measurementschema = new MeasurementSchema(fieldName, dataTypeToTSDataType(fieldType),
                            getTSEncodingByDataType(fieldType), getCompressionTypeByDataType(fieldType));

                    schema.add(measurementschema);
                }
            }
            // 声明tablet：指定单个tablet最大行数为10000，默认1024
            Tablet tablet = new Tablet(deviceId, schema, 30000);

            // 针对单独tablet添加value值
            // 可同时为多个度量插入若干各dataPoint(支持留空白，会自添空值)，添加value指明度量名即可
            int index = 0;
            for (DBVal dbVal : DBValList) {
                tablet.addTimestamp(index, dbVal.getUtcTimeMilliSeconds());

                List<String> fieldNames = dbVal.getFieldNames();
                List<DataType> fieldTypes = dbVal.getFieldTypes();
                List<Object> fieldValues = dbVal.getFieldValues();

                for (int i = 0; i < DBValList.get(0).getFieldSize(); i++) {
                    String fieldName = fieldNames.get(i);
                    DataType fieldType = fieldTypes.get(i);
                    Object fieldValue = fieldValues.get(i);

                    //TODO 获取解析的fieldValue值
                    if (fieldType == DataType.BOOLEAN) {
                        boolean booleanValue = Boolean.parseBoolean(fieldValue.toString());
                        tablet.addValue(fieldName, index, booleanValue);
                    } else if (fieldType == DataType.INT32) {
                        double doubleValue = Double.parseDouble(fieldValue.toString());
                        int intValue = (int) doubleValue;
                        tablet.addValue(fieldName, index, intValue);
                    } else if (fieldType == DataType.INT64) {
                        long longValue = Long.parseLong(fieldValue.toString());
                        tablet.addValue(fieldName, index, longValue);
                    } else if (fieldType == DataType.FLOAT) {
                        float floatValue = Float.parseFloat(fieldValue.toString());
                        tablet.addValue(fieldName, index, floatValue);
                    } else if (fieldType == DataType.DOUBLE) {
                        double doubleValue = Double.parseDouble(fieldValue.toString());
                        tablet.addValue(fieldName, index, doubleValue);
                    } else if (fieldType == DataType.TEXT) {
                        String stringValue = fieldValue.toString();
                        tablet.addValue(fieldName, index, stringValue);
                    } else {
                        //TODO 抛出解析异常
                    }
                }

                index++;
            }
            // Bitmap
            tablet.initBitMaps();
            tablet.rowSize += DBValList.size();

            // 将tablet添加进Map
            tablets.put(deviceId, tablet);

            //TODO 将发送数据指令置于此，避免二次切割操作
            if (tablets.size() >= chunkNum) {
                totalTabletSize += tablets.size();
                logger.info(String.format("current tablets size:[%d]", tablets.size()));
                // 执行插入数据函数
                SpentTimeCalculator spentTimeCalculator = SpentTimeCalculator.create().begin();
                IotdbDBApiEntry iotdbDbApiEntry = new IotdbDBApiEntry(host, Globals.PORT);
                iotdbDbApiEntry.insertTablets(tablets);
                spentTimeCalculator.end();
                logger.info(String.format("spend time:[%d](ms)", spentTimeCalculator.getSpendTime()));

                tablets.clear();
            }
        }

        // 最后一批数据
        if (tablets.size() > 0) {
            totalTabletSize += tablets.size();
            logger.info(String.format("last tablets size:[%d]", tablets.size()));
            // 执行插入数据函数
            SpentTimeCalculator spentTimeCalculator = SpentTimeCalculator.create().begin();
            IotdbDBApiEntry iotdbDbApiEntry = new IotdbDBApiEntry(host, Globals.PORT);
            iotdbDbApiEntry.insertTablets(tablets);
            spentTimeCalculator.end();
            logger.info(String.format("spend time:[%d](ms)", spentTimeCalculator.getSpendTime()));

            tablets.clear();
        }

        logger.info(String.format("all tablets size is:[%d]", totalTabletSize));
    }


    /**
     * @param chunkMap
     * @param chunkNum
     * @return java.util.Map<java.lang.String, org.apache.iotdb.tsfile.write.record.Tablet>
     * @description 该函数将指定Map对象分段返回
     * @dateTime 2023/2/1 20:28
     */
    public static Map<String, Tablet> mapChunk(Map<String, Tablet> chunkMap, int chunkNum) {
        Map<String, Tablet> result = new HashMap<>();
        if (chunkMap.isEmpty()) {
            logger.info(String.format("chunkMap is empty!"));
            return result;
        }

        Set<String> keySet = chunkMap.keySet();
        Iterator<String> iterator = keySet.iterator();

        int i = 1;
        while (iterator.hasNext()) {
            String keyNext = iterator.next();
            result.put(keyNext, chunkMap.get(keyNext));
            iterator.remove();

            if (i++ >= chunkNum) {
                logger.info(String.format("result size:%d", result.size()));
                return result;
            }
        }

        logger.info(String.format("last result size:%d", result.size()));
        return result;
    }
}
