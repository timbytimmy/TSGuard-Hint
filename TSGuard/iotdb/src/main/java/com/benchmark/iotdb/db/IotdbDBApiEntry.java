package com.benchmark.iotdb.db;

import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.ApiEntry;
import com.benchmark.commonUtil.CommonUtil;
import com.benchmark.commonUtil.SpentTimeCalculator;
import com.benchmark.constants.AggFunctionType;
import com.benchmark.constants.DataType;
import com.benchmark.constants.ValueStatus;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.AggCountResult;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PerformanceEntity;
import com.benchmark.iotdb.iotdbUtil.CommonPointDataReader;
import com.benchmark.iotdb.iotdbUtil.ParseUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.text.ParseException;
import java.util.*;

@Slf4j
public class IotdbDBApiEntry implements ApiEntry {
    private String host;
    private int port;
    private Session session;
    private long lookback = 365l * 24 * 60 * 60 * 1000 * 4;   // 定义“最近时间”
    private int slowQueryTimeThrehold = 20000;      // 单位毫秒，低时间告警阈值
    private int splitSizeRealTime = 1000;              // 分片大小
    private static final int chunkNum = 200;                     // 分批发送数据大小

    /**
     * @param host 主机
     * @param port 端口
     * @return
     * @description 初始化DBApiEntry
     * @dateTime 2022/11/29 19:15
     */
    public IotdbDBApiEntry(String host, int port) {
        this.host = host;
        this.port = port;
        this.session = new Session.Builder()
                .host(this.host)
                .port(this.port)
                .username("root")
                .password("root")
                .build();
        log.info(String.format("create IotdbDBApiEntry host:%s port:%s", this.host, this.port));
    }

    public IotdbDBApiEntry(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.session = new Session.Builder()
                .host(this.host)
                .port(this.port)
                .username(userName)
                .password(password)
                .build();
        log.info(String.format("create IotdbDBApiEntry host:%s port:%s", this.host, this.port));
    }

    /**
     * @param method
     * @param query
     * @param spendTime
     * @return void
     * @description 查询时间超过查询阈值时告警
     * @dateTime 2022/11/30 17:18
     */
    private void logSlowQuery(String method, String query, long spendTime) {
        if (spendTime > this.slowQueryTimeThrehold) {
            log.error(String.format("slowQuery spendTime:%d(ms) method:%s query:%s",
                    spendTime, method, query));
        }
    }

    /**
     * @return void
     * @description 连接session
     * @dateTime 2022/11/29 18:36
     */
    public void open() throws IoTDBConnectionException {
        this.session.open();
        log.info(String.format("open session host:%s port:%d", this.host, this.port));
    }

    /**
     * @return void
     * @description 关闭session
     * @dateTime 2022/11/29 18:37
     */
    public void close() throws IoTDBConnectionException {
        this.session.close();
//        log.info(String.format("close session host:%s port:%d", this.host, this.port));
    }

    /**
     * @param sql
     * @return org.apache.iotdb.session.SessionDataSet
     * @description 根据sql指令查询Last数据并返回
     * @dateTime 2022/12/1 20:30
     */
    public List<PointData> queryPointLastData(String sql) {
        List<PointData> result = new ArrayList<>();
        try {
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            // 该函数执行时输出SQL语句日志，不符合要求
            SessionDataSet sessionDataSet = this.session.executeQueryStatement(sql);
            Hashtable<String, SortedMap<Long, PointData>> pointDataHashtable = ParseUtil.parseForQueryLast(sessionDataSet);
            this.close();
            stc.end();

            //依据pointDataHashtable获取result
            for (String pointID : pointDataHashtable.keySet()) {
                SortedMap timeSery = pointDataHashtable.get(pointID);

                if (!timeSery.isEmpty()) {
                    PointData pointData = (PointData) timeSery.values().iterator().next();
                    result.add(pointData);
                }
            }
            log.info(String.format("queryPointLastData queryPointLastDataSize:%d spendTime:%d(ms)", result.size(),
                    stc.getSpendTime()));
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("queryPointLastData exception:%s", e));
        }
        return result;
    }

    /**
     * @param timeSeriesList 时间序列列表
     * @return boolean
     * @description 插入多个时间序列
     * @dateTime 2022/11/29 18:23
     */
    public boolean createTimeseries(List<TSVal> timeSeriesList) {
        if (timeSeriesList == null) {
            return false;
        }

        try {
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            for (int i = 0; i < timeSeriesList.size(); i++) {
                TSVal pointData = timeSeriesList.get(i);
                this.session.createTimeseries(pointData.getPath(), pointData.getDataType(), pointData.getEncoding(),
                        pointData.getCompressor());
            }
            this.close();
            stc.end();
            log.info(String.format("createTimeseries createTimeseriesSize:%d spendTime:%d(ms)", timeSeriesList.size(),
                    stc.getSpendTime()));
            return true;
        } catch (Exception e) {
            log.error(String.format("createTimeseries exception:%s", e));
            return false;
        }
    }

    /**
     * @param tablet
     * @return boolean
     * @description 以Tablet格式插入一系列时序数据
     * @dateTime 2022/11/29 20:43
     */
    public boolean insertTablet(Tablet tablet) {
        if (tablet == null) {
            return false;
        }

        try {
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            this.session.insertTablet(tablet);
            this.close();
            stc.end();
            log.info(String.format("InsertTablet insertTabletSize:%d spendTime:%d(ms)", tablet.rowSize,
                    stc.getSpendTime()));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error(String.format("InsertTablet exception:%s", e));
            return false;
        }
    }

    /**
     * @param tablets
     * @return long 操作耗时
     * @description 插入多个tablet
     * @dateTime 2023/1/11 21:51
     */
    public boolean insertTablets(Map<String, Tablet> tablets) {
        if (tablets == null) {
            log.error(String.format("tablets is null or empty!"));
            return false;
        }

        try {
            this.session.insertTablets(tablets);
            return true;
        } catch (Exception e) {
            log.error(String.format("InsertTablets exception:%s", e));
            return false;
        }
    }

    /**
     * @param record
     * @return boolean
     * @description 插入一条record，即一个设备一个时间戳下多个测点的数据。
     * @dateTime 2022/11/29 23:17
     */
    public boolean insertRecord(Record record) {
        if (record == null) {
            return false;
        }

        try {
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            this.session.insertRecord(record.getPrefixPath(), record.getTimestamp(),
                    record.getMeasurements(), record.getTypes(), record.getValues());
            this.close();
            stc.end();
            log.info(String.format("insertRecord insertRecordSize:[%d] spendTime:[%d](ms)",
                    record.getValues().size(), stc.getSpendTime()));
            return true;
        } catch (Exception e) {
            log.error(String.format("insertRecord exception:%s", e));
            return false;
        }
    }

    /**
     * @param records
     * @return boolean
     * @description 插入多条record
     * @dateTime 2022/11/30 9:09
     */
    public boolean insertMultiRecord(List<Record> records) {
        if (records == null) {
            return false;
        }

        try {
            List<String> deviceIds = new ArrayList<>();
            List<Long> times = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            List<List<TSDataType>> typesList = new ArrayList<>();
            List<List<Object>> valuesList = new ArrayList<>();
            long totalNum = 0;

            for (Record record : records) {
                deviceIds.add(record.getPrefixPath());
                times.add(record.getTimestamp());
                measurementsList.add(record.getMeasurements());
                typesList.add(record.getTypes());
                valuesList.add(record.getValues());
                totalNum += record.getValues().size();
            }

            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            this.session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
            this.close();
            stc.end();
            log.info(String.format("insertMultiRecord insertMultiRecordSize:%d spendTime:%d(ms)",
                    totalNum, stc.getSpendTime()));
            return true;
        } catch (Exception e) {
            log.error(String.format("insertMultiRecord exception:%s", e));
            return false;
        }
    }

    /**
     * @param paths 度量名+tags+fields
     * @return java.util.Hashtable<java.lang.String, db.PointData>
     * @description 根据给定的paths查询最近的数据
     * @dateTime 2022/11/30 17:10
     */
    public Hashtable<String, PointData> queryLast(List<String> paths) {
        Hashtable<String, PointData> results = new Hashtable<>();
        if (paths == null) {
            return results;
        }

        try {
            long endDate = System.currentTimeMillis();
            long beginDate = endDate - lookback;
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();

            this.open();
            SessionDataSet sessionDataSet = this.session.executeLastDataQuery(paths, beginDate);
            Hashtable<String, SortedMap<Long, PointData>> pointDataHashtable = ParseUtil.parseForQueryLast(sessionDataSet);
            this.close();

            //依据pointDataHashtable获取result，过程和influx一致
            for (String pointID : pointDataHashtable.keySet()) {

                SortedMap timeSery = pointDataHashtable.get(pointID);
                if (!timeSery.isEmpty()) {
                    PointData pointData = (PointData) timeSery.values().iterator().next();
                    results.put(pointID, pointData);
                }
            }
            stc.end();
            log.info(String.format("queryLast id:%s beginDate:%s endDate:%s spendTime:%d(ms)",
                    paths, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate),
                    stc.getSpendTime()));
        } catch (Exception e) {
            log.error(String.format("queryLast exception:%s", e));
        }

        return results;
    }

    /**
     * @param paths     measurementName + tags + field
     * @param beginDate
     * @param endDate
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, db.PointData>>
     * @description 根据给定paths，查询一段时间数据
     * @dateTime 2022/11/30 17:22
     */
    public Hashtable<String, List<PointData>> queryPointDataTimeRange(List<String> paths, long beginDate, long endDate) {
        Hashtable<String, List<PointData>> results = new Hashtable<>();

        try {
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            SessionDataSet sessionDataSet = this.session.executeRawDataQuery(paths, beginDate, endDate);
            Hashtable<String, SortedMap<Long, PointData>> pointDataHashtable = ParseUtil.parseForQueryTimeRange(sessionDataSet);
            this.close();
            stc.end();
            log.info(String.format("queryPointDataTimeRange beginDate:%s endDate:%s pointCount:%d spendTime:%d(ms)",
                    CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate),
                    paths.size(), stc.getSpendTime()));

            // 暂不设置查询时间超出阈值函数
            // logSlowQuery("queryPointDataTimeRange", paths.toString(), stc.getSpendTime());

            //依据pointDataHashtable获取result，过程和influx一致
            for (String pointID : pointDataHashtable.keySet()) {
                SortedMap timeSery = pointDataHashtable.get(pointID);
                List<PointData> pointDataList = new ArrayList<>(timeSery.values());
                results.put(pointID, pointDataList);
            }
        } catch (Exception e) {
            log.error(String.format("queryPointDataTimeRange exception:%s", e));
        }
        return results;
    }

    // TODO 原apiEntry是否需要增加最新点查询功能？

    /**
     * @param dbValParam
     * @return DBVal
     * @description 根据传参DBValParam，查询最新点数据
     * @dateTime 2023/2/16 19:54
     */
    public DBVal queryLast(DBValParam dbValParam) {
        DBVal result;

        // 判断传参字符串是否为空
        if (dbValParam == null) {
            log.warn(String.format("dbValParam is null"));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.warn(String.format("tagName is empty"));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        // 获取存储路径
        List<String> paths = new ArrayList<>();
        paths.add(ParseUtil.parseDBValParamToPath(dbValParam));

        try {
            long beginDate = System.currentTimeMillis() - lookback;
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            SessionDataSet sessionDataSet = this.session.executeLastDataQuery(paths, beginDate);
            Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseUtil.parseResultToDBValsLast(
                    dbValParam.getTagName(), sessionDataSet);
            this.close();
            stc.end();

            // 转换数据结果
            if (dbValHashtable.isEmpty()) {
                return DBVal.DBValBuilder.anDBVal()
                        .withValueStatus(ValueStatus.INVALID)
                        .build();
            }

            result = dbValHashtable.values().iterator().next().values().iterator().next();

            // log.info(String.format("queryLast id:%s beginDate:%s endDate:%s spendTime:%d(ms)",
            //         paths, CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
            //         CommonUtil.uTCMilliSecondsToDateString(endDate),
            //         stc.getSpendTime()));

            return result;
        } catch (Exception e) {
            log.warn(String.format("queryLast exception:%s", e));
            return DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
        }
    }

    @Override
    public boolean isConnected() {
        try {
            String sql = "SHOW DATABASES";
            this.session.executeQueryStatement(sql);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean disConnect() {
        try {
            this.session.close();
            return true;
        } catch (Exception e) {
            log.error("关闭session失败");
            return false;
        }
    }

    @Override
    public int sendPointData(String tagName, List<DBVal> dbValList) {
        return 0;
    }

    @Override
    public int sendSinglePoint(String tagName, List<DBVal> dbValList) {
        return 0;
    }

    @Override
    public int updatePointValues(String tag, String startTime, String endTime, List<DBVal> updateDatas) throws ParseException {
        return 0;
    }

    @Override
    public boolean deletePointData(String tagName) {
        return false;
    }

    @Override
    public PerformanceEntity sendMultiPoint(@NonNull List<DBVal> dbValList) {
        return sendTabletsInbatchs(dbValList);
    }

    public PerformanceEntity sendTabletsInbatchs(List<DBVal> dbValList) {
        if (dbValList.isEmpty()) {
            log.info(String.format("driverLocationValHashtable is empty!"));
            return PerformanceEntity.builder()
                    .isSuccess(false)
                    .build();
        }

        try {
            long timeCost = 0L;
            long size = 0L;

            this.open();
            // 将DBVal按照TableName、TagName分组
            // TODO 分组导致性能问题，不可计入Benchmark计量
            Hashtable<Pair<String, String>, List<DBVal>> hashtable = CommonUtil.groupDBValByTableNameAndTagName(dbValList);
            for (Pair<String, String> pair : hashtable.keySet()) {
                String tableName = pair.getKey();
                String tagName = pair.getValue();
                List<DBVal> dbVals = hashtable.get(pair);

                // 将dbVal按照tagValue分组，针对具有多个时间戳的数据
                Hashtable<String, List<DBVal>> dbValHashtable = CommonUtil.groupDBValByTagValue(dbVals);
                Map<String, Tablet> tablets = new HashMap<>();

                for (String tagValue : dbValHashtable.keySet()) {
                    List<DBVal> dbValFragment = dbValHashtable.get(tagValue);
                    // 分组插入
                    Tablet tablet = dbValsToTablet(tableName, tagValue, dbValFragment);
                    size += tablet.rowSize;
                    // 将tablet添加进Map
                    tablets.put(tablet.deviceId, tablet);
                    if (tablets.size() >= chunkNum) {
                        // 执行插入数据函数
                        SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
                        if (!insertTablets(tablets)) {
                            return PerformanceEntity.builder()
                                    .isSuccess(false)
                                    .build();
                        }
                        stc.end();
                        timeCost += stc.getSpendTime();
                        tablets.clear();
                    }
                }

                // 最后一批数据
                if (tablets.size() != 0) {
                    // 执行插入数据函数
                    SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
                    if (!insertTablets(tablets)) {
                        return PerformanceEntity.builder()
                                .isSuccess(false)
                                .build();
                    }
                    stc.end();
                    timeCost += stc.getSpendTime();
                }
            }

            this.close();
            return PerformanceEntity.builder()
                    .timeCost(timeCost)
                    .obj(size)
                    .isSuccess(true)
                    .build();
        } catch (IoTDBConnectionException e) {
            log.error(String.format("IOTDB连接异常:%s", e));
            return PerformanceEntity.builder()
                    .isSuccess(false)
                    .build();
        }
    }

    /**
     * @param tableName
     * @param tagValue
     * @param dbVals
     * @return org.apache.iotdb.tsfile.write.record.Tablet
     * @description 将已按TableName、TagName、TagValue分组好的DBVal转换为Tablet(仅sendTabletsInbatchs调用)
     * @dateTime 2023/3/3 9:20
     */
    protected Tablet dbValsToTablet(String tableName, String tagValue, List<DBVal> dbVals) {
        // 针对个体执行插入操作（tagValue一致）
        //TODO 目前仅支持单个tag，后续扩展多tags，增加路径长度即可（目前路径命名：root+measurement+tags）
        String deviceId = String.format("root.%s.%s", tableName, tagValue);
        List<MeasurementSchema> schema = new ArrayList<>();
        {
            // measurementId可乱序，相当于set集合，表明某个field值具体的value属性;一台设备下的measurement应该保持一致
            List<String> fieldNames = dbVals.get(0).getFieldNames();
            List<DataType> fieldTypes = dbVals.get(0).getFieldTypes();
            for (int i = 0; i < dbVals.get(0).getFieldSize(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);

                MeasurementSchema measurementschema = new MeasurementSchema(fieldName,
                        CommonPointDataReader.dataTypeToTSDataType(fieldType),
                        CommonPointDataReader.getTSEncodingByDataType(fieldType),
                        CommonPointDataReader.getCompressionTypeByDataType(fieldType));

                schema.add(measurementschema);
            }
        }
        // 声明tablet：指定单个tablet最大行数为10000，默认1024
        Tablet tablet = new Tablet(deviceId, schema, 30000);

        // 针对单独tablet添加value值
        // 可同时为多个度量插入若干各dataPoint(支持留空白，会自添空值)，添加value指明度量名即可
        int index = 0;
        for (DBVal dbVal : dbVals) {
            tablet.addTimestamp(index, dbVal.getUtcTimeMilliSeconds());

            List<String> fieldNames = dbVal.getFieldNames();
            List<DataType> fieldTypes = dbVal.getFieldTypes();
            List<Object> fieldValues = dbVal.getFieldValues();

            for (int i = 0; i < dbVals.get(0).getFieldSize(); i++) {
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
        tablet.rowSize += dbVals.size();
        return tablet;
    }

    @Override
    public DBVal getRTValue(DBValParam dbValParam) {
        DBVal result;

        // 判断传参字符串是否为空
        if (dbValParam == null) {
            log.warn(String.format("dbValParam is null"));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }
        // TODO tagName数据库内标记，无法区别
        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.warn(String.format("tagName is empty"));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        // 获得查询路径
        List<String> paths = new ArrayList<>();
        paths.add(ParseUtil.parseDBValParamToPath(dbValParam));

        Hashtable<String, SortedMap<Long, DBVal>> hashtable = queryTimeRange(dbValParam.getTagName(), paths,
                System.currentTimeMillis() - 500, System.currentTimeMillis() + 500);

        if (hashtable.isEmpty()) {
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        // 从hashTable获取DBVal
        result = hashtable.get(dbValParam.getTagValue()).values().iterator().next();
        return result;
    }

    @Override
    public List<DBVal> getRTValueList(List<DBValParam> dbValParamList) {
        List<DBVal> results = new ArrayList<>();
        if (dbValParamList == null) {
            return results;
        }

        // 将DBValParams分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            // 针对不同tableName 和 tagName分组查找数据
            List<String> paths = ParseUtil.parseDBValParamsToPaths(tableName, dbValParams);
            Hashtable<String, SortedMap<Long, DBVal>> resHashtable = queryTimeRange(tagName, paths,
                    System.currentTimeMillis() - 500, System.currentTimeMillis() + 500);

            if (resHashtable.isEmpty()) continue;

            // 将Hashtable结果按tagValue传参顺序转为DBValList
            Set<String> tagValues = new LinkedHashSet<>();
            for (DBValParam dbValParam : dbValParams) {
                if (!StringUtils.isBlank(dbValParam.getTagValue()))
                    tagValues.add(dbValParam.getTagValue());
            }

            results.addAll(turnListOrdered(tagValues, resHashtable));
        }

        return results;
    }

    public DBVal getLastValue(DBValParam dbValParam) {
        DBVal result;

        // 判断传参字符串是否为空
        if (dbValParam == null) {
            log.warn("dbValParam is null");
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }
        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.warn("tagName is empty");
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        // 获得查询路径
        List<String> paths = new ArrayList<>();
        paths.add(ParseUtil.parseDBValParamToPath(dbValParam));

        Hashtable<String, SortedMap<Long, DBVal>> hashtable = queryMultiLast(paths, dbValParam.getTagName());

        if (hashtable.isEmpty()) {
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        // 从hashTable获取DBVal
        result = hashtable.get(dbValParam.getTagValue()).values().iterator().next();
        return result;
    }

    @Override
    public List<DBVal> getLastValueList(List<DBValParam> dbValParamList) {
        List<DBVal> results = new ArrayList<>();
        if (dbValParamList == null) {
            return results;
        }

        // 将DBValParams分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            // 针对不同tableName 和 tagName分组查找数据
            List<String> paths = ParseUtil.parseDBValParamsToPaths(tableName, dbValParams);
            Hashtable<String, SortedMap<Long, DBVal>> resHashtable = queryMultiLast(paths, tagName);

            if (resHashtable.isEmpty()) continue;

            // 将Hashtable结果按tagValue传参顺序转为DBValList
            Set<String> tagValues = new LinkedHashSet<>();
            for (DBValParam dbValParam : dbValParams) {
                if (!StringUtils.isBlank(dbValParam.getTagValue()))
                    tagValues.add(dbValParam.getTagValue());
            }

            results.addAll(turnListOrdered(tagValues, resHashtable));
        }

        return results;
    }

    /**
     * @param paths
     * @return java.util.Hashtable<java.lang.String, java.util.SortedMap < java.lang.Long, com.benchmark.entity.DBVal>>
     * @description 根据提供路径，实时数据查询，即最近点查询
     * @dateTime 2023/3/17 16:37
     */
    protected Hashtable<String, SortedMap<Long, DBVal>> queryMultiLast(List<String> paths, String tagName) {
        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = new Hashtable<>();
        try {
            long beginDate = System.currentTimeMillis() - lookback;
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            SessionDataSet sessionDataSet = this.session.executeLastDataQuery(paths, beginDate);
            dbValHashtable = ParseUtil.parseResultToDBValsLast(tagName, sessionDataSet);
            this.close();
            stc.end();
            return dbValHashtable;
        } catch (Exception e) {
            log.warn(String.format("queryLast exception:%s", e));
            return dbValHashtable;
        }
    }

    /**
     * @param tagValues
     * @param dbValHashtable
     * @return java.util.List<DBVal>
     * @description 将dbValHashtable按照传参tagValues顺序转为dbVals
     * @dateTime 2023/2/17 9:22
     */
    protected List<DBVal> turnListOrdered(Set<String> tagValues,
                                          Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable) {
        List<DBVal> dbVals = new ArrayList<>();
        for (String tagValue : tagValues) {
            if (dbValHashtable.containsKey(tagValue)) {
                dbVals.addAll(dbValHashtable.get(tagValue).values());
            } else {
                log.info(String.format("result is empty: tagValue=%s", tagValue));
            }
        }
        return dbVals;
    }

    @Override
    public List<DBVal> getRTValueListUseSplit(List<DBValParam> dbValParamList) {
        List<DBVal> results = new ArrayList<>();
        if (dbValParamList == null) {
            log.warn(String.format("dbValParams is empty!"));
            return results;
        }

        // 将dbValParamList依据tableName 和 tagName分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            // TODO 由于解析结果包含tableName，此处是否仅需按tagName分组？
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            Hashtable<String, SortedMap<Long, DBVal>> resHashtable = new Hashtable<>();
            // 针对每一组dbValParams
            int times = dbValParams.size() / splitSizeRealTime;
            if ((dbValParams.size() % splitSizeRealTime) != 0) times++;

            int fromIndex, toIndex;
            for (int i = 0; i < times; i++) {
                fromIndex = i * splitSizeRealTime;
                toIndex = (i + 1) * splitSizeRealTime;
                if (toIndex > dbValParams.size()) toIndex = dbValParams.size();

                // 查找数据
                List<String> paths = ParseUtil.parseDBValParamsToPaths(tableName, dbValParams);

                Hashtable<String, SortedMap<Long, DBVal>> resHashtablePartial = queryTimeRange(tagName, paths,
                        System.currentTimeMillis(), System.currentTimeMillis() + 1);

                resHashtable.putAll(resHashtablePartial);
            }
            if (resHashtable.isEmpty()) continue;

            // 将本组Hashtable结果按tagValue传参顺序转为DBValList
            Set<String> tagValues = new LinkedHashSet<>();
            for (DBValParam dbValParam : dbValParams) {
                if (!StringUtils.isBlank(dbValParam.getTagValue()))
                    tagValues.add(dbValParam.getTagValue());
            }

            results.addAll(turnListOrdered(tagValues, resHashtable));
        }

        return results;
    }

    @Override
    public Map<String, List<DBVal>> getHistMultiTagValsFast(List<DBValParam> dbValParamList, long start,
                                                            long end, int step) {
        return null;
    }

    @Override
    public List<DBVal> getHistSnap(DBValParam dbValParam, long start, long end, long step) {
        return null;
    }

    @Override
    public List<DBVal> getHistSnap(DBValParam dbValParam, long startTime, long endTime, long period, long lookBack) {
        return null;
    }

    @Override
    public List<DBVal> getHistRaw(DBValParam dbValParam, long tStart, long tEnd) {
        List<DBVal> dbValList = new ArrayList<>();
        if (dbValParam == null) {
            log.warn(String.format("dbValParam is null!"));
            return dbValList;
        }
        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.warn(String.format("tagName is empty"));
            return dbValList;
        }
        if (tStart > tEnd) {
            log.warn(String.format("time range is empty"));
            return dbValList;
        }

        // 查找数据
        List<String> paths = new ArrayList<>();
        paths.add(ParseUtil.parseDBValParamToPath(dbValParam));
        Hashtable<String, SortedMap<Long, DBVal>> resHashtable = queryTimeRange(dbValParam.getTagName(),
                paths, tStart, tEnd + 1);

        dbValList.addAll(resHashtable.get(dbValParam.getTagValue()).values());
        return dbValList;
    }

    @Override
    public List<DBVal> getHistRaw(List<DBValParam> dbValParamList, long tStart, long tEnd) {
        List<DBVal> results = new ArrayList<>();
        if (dbValParamList == null) {
            return results;
        }

        // 将DBValParams分组
        Hashtable<Pair<String, String>, List<DBValParam>> hashtable = CommonUtil.groupDBValParamByTableNameAndTagName(dbValParamList);
        for (Pair<String, String> pair : hashtable.keySet()) {
            String tableName = pair.getKey();
            String tagName = pair.getValue();
            List<DBValParam> dbValParams = hashtable.get(pair);

            // 针对不同tableName 和 tagName分组查找数据
            List<String> paths = ParseUtil.parseDBValParamsToPaths(tableName, dbValParams);

            Hashtable<String, SortedMap<Long, DBVal>> resHashtable = queryTimeRange(tagName, paths,
                    tStart, tEnd + 1);

            if (resHashtable.isEmpty()) continue;

            // 将Hashtable结果按tagValue传参顺序转为DBValList,当传参为*时,如何执行排序？
            // Set<String> tagValues = new LinkedHashSet<>();
            // for(DBValParam dbValParam : dbValParams) {
            //     if(StringUtils.isBlank(dbValParam.getTagValue())) {
            //         return results.addAll()
            //     }
            //     else {
            //         tagValues.add(dbValParam.getTagValue());
            //     }
            // }
            // results.addAll(turnListOrdered(tagValues, resHashtable));

            for (String tagValue : resHashtable.keySet()) {
                List<DBVal> dbVals = new ArrayList<>(resHashtable.get(tagValue).values());
                results.addAll(dbVals);
            }
        }
        return results;
    }

    @Override
    public List<DBVal> getHistInstantRaw(List<DBValParam> dbVals, long time) {
        return null;
    }

    /**
     * @param paths
     * @param beginDate
     * @param endDate
     * @return Hashtable<tagValue, SortedMap < timestamp, DBVal>> 返回结果tagValue是无序的
     * @description 通用查询数据，查询单一tagName，指定paths下，指定时间范围数据[范围：左闭右开]并以DBVal列表返回
     * @dateTime 2023/2/16 20:18
     */
    protected Hashtable<String, SortedMap<Long, DBVal>> queryTimeRange(String tagName, List<String> paths,
                                                                       long beginDate, long endDate) {
        Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = new Hashtable<>();
        try {
            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            SessionDataSet sessionDataSet = this.session.executeRawDataQuery(paths, beginDate, endDate);
            dbValHashtable = ParseUtil.parseResultToDBValsTimeRange(tagName, sessionDataSet);
            this.close();
            stc.end();

            log.info(String.format("queryPointDataTimeRange beginDate:%s endDate:%s pointCount:%d spendTime:%d(ms)",
                    CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate),
                    paths.size(), stc.getSpendTime()));

            // 暂不设置查询时间超出阈值函数
            // logSlowQuery("queryPointDataTimeRange", paths.toString(), stc.getSpendTime());

        } catch (Exception e) {
            log.error(String.format("queryPointDataTimeRange exception:%s", e));
        }

        // for (SortedMap<Long, DBVal> map : dbValHashtable.values()) {
        //     for (DBVal val : map.values())
        //         System.out.println(val.toString());
        // }

        return dbValHashtable;
    }


    @Override
    public DBVal getRTMinValue(DBValParam dbValParam, long startTime, long endTime) {
        return getAggValueOverTime(AggFunction.MIN.getFunc(), dbValParam, startTime, endTime);
    }

    @Override
    public DBVal getRTMaxValue(DBValParam dbValParam, long startTime, long endTime) {
        return getAggValueOverTime(AggFunction.MAX.getFunc(), dbValParam, startTime, endTime);
    }

    @Override
    public DBVal getRTAvgValue(DBValParam dbValParam, long startTime, long endTime) {
        return getAggValueOverTime(AggFunction.AVG.getFunc(), dbValParam, startTime, endTime);
    }

    @Override
    public AggCountResult getRTCountValue(DBValParam dbValParam, long startTime, long endTime) {
        DBVal dbVal = getAggValueOverTime(AggFunction.COUNT.getFunc(), dbValParam, startTime, endTime);
        if (dbVal.isValueValid()) {
            return AggCountResult.AggCountResultBuilder.anResult()
                    .withTableName(dbVal.getTableName())
                    .withTagName(dbVal.getTagName())
                    .withTagValue(dbVal.getTagValue())
                    .withCount((Long) dbVal.getFieldValues().iterator().next())
                    .build();
        }

        return AggCountResult.AggCountResultBuilder.anResult().setResultInvalid().build();
    }

    /**
     * @param aggMethod
     * @param dbValParam
     * @param beginDate
     * @param endDate
     * @return java.lang.String
     * @description 按照给定条件生成聚合查询语句(目前仅支持单字段查询) *表达式能查询所有字段数据
     * @dateTime 2023/3/19 16:26
     */
    protected String genAggSQL(String aggMethod, DBValParam dbValParam, long beginDate, long endDate) {
        return String.format("select %s(%s) from " +
                        "root.%s.%s where timestamp >= %d and timestamp <= %d", aggMethod, dbValParam.getFieldName(),
                dbValParam.getTableName(), dbValParam.getTagValue(), beginDate, endDate);
    }

    /**
     * @param aggMethod
     * @param dbValParam
     * @param beginDate
     * @param endDate
     * @return dataObject.DBVal
     * @description 按照聚合粒度对某时序某时间段数据执行指定聚合查询
     * @dateTime 2023/2/26 17:10
     */
    protected DBVal getAggValueOverTime(String aggMethod, DBValParam dbValParam, long beginDate, long endDate) {

        DBVal result;
        if (beginDate > endDate) {
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }
        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.info(String.format("tagName is empty!"));
            result = DBVal.DBValBuilder.anDBVal()
                    .withValueStatus(ValueStatus.INVALID)
                    .build();
            return result;
        }

        // 针对dbValParam参数FieldName进行模糊查询处理， 即检索全部字段
        if (dbValParam.getFieldName() == null || dbValParam.getFieldName().length() == 0) {
            dbValParam.setFieldName("*");
        }

        try {
            List<String> paths = new ArrayList<>();
            paths.add(ParseUtil.parseDBValParamToPath(dbValParam));
            String sql = genAggSQL(aggMethod, dbValParam, beginDate, endDate);

            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            SessionDataSet sessionDataSet = this.session.executeQueryStatement(sql);
            // 解析数据
            Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseUtil.parseAggResultToDBVal(dbValParam.getTagName(), sessionDataSet);
            this.close();
            stc.end();

            log.info(String.format("queryPointDataTimeRange beginDate:%s endDate:%s pointCount:%d spendTime:%d(ms)",
                    CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate),
                    paths.size(), stc.getSpendTime()));

            if (!dbValHashtable.isEmpty()) {
                result = dbValHashtable.values().iterator().next().values().iterator().next();
                return result;
            }
        } catch (Exception e) {
            log.warn(String.format("retrieval error: tableName=%s, tagName=%s, aggMethod=%s",
                    dbValParam.getTableName(), dbValParam.getTagName(), aggMethod));
        }

        result = DBVal.DBValBuilder.anDBVal()
                .withValueStatus(ValueStatus.INVALID)
                .build();
        return result;
    }

    // 生成降采样查询SQL语句
    protected String genDownSamplingQuerySQL(String aggMethod, long timeGranularity, DBValParam dbValParam,
                                             long beginDate, long endDate) {
        return String.format("select %s(%s) from root.%s.%s group by ([%d, %d), %ds)", aggMethod,
                dbValParam.getFieldName(), dbValParam.getTableName(), dbValParam.getTagValue(),
                beginDate, endDate, timeGranularity / 1000);
    }

    // 降采样查询
    @Override
    public List<DBVal> downSamplingQuery(AggFunctionType aggFunctionType, long timeGranularity,
                                         DBValParam dbValParam, long beginDate, long endDate) {
        List<DBVal> results = new ArrayList<>();

        if (beginDate > endDate) {
            return results;
        }
        if (StringUtils.isBlank(dbValParam.getTagName())) {
            log.info(String.format("tagName is empty!"));
            return results;
        }

        // 针对dbValParam参数FieldName进行模糊查询处理， 即检索全部字段
        if (dbValParam.getFieldName() == null || dbValParam.getFieldName().length() == 0) {
            dbValParam.setFieldName("*");
        }

        try {
            String sql = genDownSamplingQuerySQL(AggFunction.valueOf(aggFunctionType.getFunc()).getFunc(), timeGranularity,
                    dbValParam, beginDate, endDate);

            SpentTimeCalculator stc = SpentTimeCalculator.create().begin();
            this.open();
            SessionDataSet sessionDataSet = this.session.executeQueryStatement(sql);
            // 解析数据
            Hashtable<String, SortedMap<Long, DBVal>> dbValHashtable = ParseUtil.parseResultToDBValsTimeRange(
                    dbValParam.getTagName(), sessionDataSet);
            this.close();
            stc.end();

            if (dbValHashtable.isEmpty()) return results;

            // 转dbValHashTable至DBValList
            dbValHashtable.values().iterator().forEachRemaining(timeSeries -> {
                results.addAll(timeSeries.values());
            });

            log.info(String.format("queryPointDataTimeRange beginDate:%s endDate:%s pointCount:%d spendTime:%d(ms)",
                    CommonUtil.uTCMilliSecondsToDateStringWithMs(beginDate),
                    CommonUtil.uTCMilliSecondsToDateString(endDate),
                    results.size(), stc.getSpendTime()));
        } catch (Exception e) {
            log.warn(String.format("retrieval error: tableName=%s, tagName=%s, aggMethod=%s",
                    dbValParam.getTableName(), dbValParam.getTagName(), aggFunctionType.getFunc()));
            return new ArrayList<>();
        }

        return results;
    }

    public void executeNonQueryStatement(String sql) throws IoTDBConnectionException, StatementExecutionException {
        log.info("[IOTDB_SESSION_NONQUERY] {}", sql); // to view SELECT log
        this.session.executeNonQueryStatement(sql);
    }

    public SessionDataSet executeQueryStatement(String sql) throws IoTDBConnectionException, StatementExecutionException {
        log.info("[IOTDB_SESSION_QUERY] {}", sql);  //to view SELECT log
        return this.session.executeQueryStatement(sql);
    }
}
