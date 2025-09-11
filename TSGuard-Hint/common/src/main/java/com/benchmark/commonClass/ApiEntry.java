package com.benchmark.commonClass;

import com.benchmark.constants.AggFunctionType;
import com.benchmark.dto.DBValParam;
import com.benchmark.entity.AggCountResult;
import com.benchmark.entity.DBVal;
import com.benchmark.entity.PerformanceEntity;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * 时序数据库方法模板
 *
 * @author
 */
public interface ApiEntry {
    // EDOS库之间可能用的SUCCESS CODE 不一样, 所以定义在这里, 统一引用进行判断
    public static final short TRIUMPH_CODE = 3;
    public static final short DEFEAT_CODE = 4;
    // TODO 原apiEntry是否需要增加最新点查询功能？
    // TODO 各数据库存活状态

    /**
     * 测试是否时序数据源是否链接, 如http这种, 不是长连接的, 直接return true 即可
     *
     * @return
     */
    public abstract boolean isConnected();

    /**
     * 关闭数据源连接, 如http这种, 不属于长连接的, renturn true即可
     *
     * @return
     */
    public abstract boolean disConnect();


    /**
     * 写入指定tagValue 的 多个值, 含时间, dbVals 只会有tagName的数据
     *
     * @param tagName DCS1_A
     * @param dbVals  [{tagName: DCS1_FH, utcTime: 2000025121(秒数), statu: TRIUMPH_CODE}, {tagName: DCS1_FH, utcTime: 2000025121(秒数), statu: TRIUMPH_CODE}, ]
     * @return
     */
    public abstract int sendPointData(String tagName, List<DBVal> dbVals);

    /**
     * 实时写入, 含时间戳, 默认写入的值
     *
     * @param tagName
     * @param dbVals
     * @return
     */
    public abstract int sendSinglePoint(String tagName, List<DBVal> dbVals);

    /**
     * 写入多个tagName 组合成的 dbVals, 根据时序数据库类型, 确认是否需要分割, 不带时间戳, 默认实时时间戳
     *
     * @param dbVals
     * @return
     */
    public abstract PerformanceEntity sendMultiPoint(List<DBVal> dbVals);

    /**
     * 更新单点的多条数据方法
     *
     * @return
     */
    public abstract int updatePointValues(String tag, String startTime, String endTime, List<DBVal> updateDatas) throws ParseException;

    /**
     * 删除指定时间段的点数据, 如果时序数据库不支持, return true 即可
     *
     * @param tagValue
     * @return
     */
    public abstract boolean deletePointData(String tagValue);

    /**
     * 获取tagValue的实时值（目前influxdb是查询点最近值）
     *
     * @param dbValParam
     * @return
     */
    public abstract DBVal getRTValue(DBValParam dbValParam);

    /**
     * 获取tagValue的最近点
     *
     * @param dbValParam
     * @return
     */
    public abstract DBVal getLastValue(DBValParam dbValParam);

    public abstract List<DBVal> getLastValueList(List<DBValParam> dbValParams);

    /**
     * 获取多个tagValue 的实时值
     *
     * @param dbValParams
     * @return
     */
    public abstract List<DBVal> getRTValueList(List<DBValParam> dbValParams);

    /**
     * 针对某一特定tagName，获取多个tagValue的实时值
     *
     * @param dbValParams
     * @return
     */
    public abstract List<DBVal> getRTValueListUseSplit(List<DBValParam> dbValParams);

    /**
     * @param dbValParams
     * @param start
     * @param end
     * @param step
     * @return java.util.Map<java.lang.String, java.util.List < db.temp.DBVal>>
     * @description 指定时间段下，指定采样间隔查询结果。key: tagValue
     * @dateTime 2023/2/8 17:01
     */
    public abstract Map<String, List<DBVal>> getHistMultiTagValsFast(List<DBValParam> dbValParams, long start, long end, int step);

    /**
     * 根据间隔取 某个点的值
     *
     * @param dbVal
     * @param start
     * @param end
     * @param step
     * @return
     */
    public abstract List<DBVal> getHistSnap(DBValParam dbVal, long start, long end, long step);

    /**
     * 根据间隔取 某个点的值 该方法将向前查找一定范围内(lookBack)的数据并填充，若向前查找无数据，则在该点设置无效值
     * Attention please, this method will cost a lot, if data sparsity exists with a small period.
     *
     * @param dbValParam
     * @param startTime
     * @param endTime
     * @param period
     * @param lookBack
     * @return
     */
    public abstract List<DBVal> getHistSnap(DBValParam dbValParam, long startTime, long endTime, long period, long lookBack);

    /**
     * 获取 tStart -tEnd 区间内tagName的所有值
     *
     * @param dbVal
     * @param tStart
     * @param tEnd
     * @return
     */
    public abstract List<DBVal> getHistRaw(DBValParam dbVal, long tStart, long tEnd);

    /**
     * 针对同一tagName，不同fieldName、不同tagValue，获取多个点 指定区间内的值
     *
     * @param dbVals
     * @param tStart
     * @param tEnd
     * @return
     */
    public abstract List<DBVal> getHistRaw(List<DBValParam> dbVals, long tStart, long tEnd);

    /**
     * 获取历史断面数据
     *
     * @param dbVals
     * @param time
     * @return
     */
    public abstract List<DBVal> getHistInstantRaw(List<DBValParam> dbVals, long time);

    /**
     * 获取区间内tagName点 最小的值
     *
     * @param dbValParam
     * @param startTime
     * @param endTime
     * @return
     */
    public abstract DBVal getRTMinValue(DBValParam dbValParam, long startTime, long endTime);

    /**
     * 获取区间内tagName点 最大的值
     *
     * @param dbValParam
     * @param startTime
     * @param endTime
     * @return
     */
    public abstract DBVal getRTMaxValue(DBValParam dbValParam, long startTime, long endTime);

    /**
     * 获取区间内tagName点 平均值
     *
     * @param dbValParam
     * @param startTime
     * @param endTime
     * @return
     */
    public abstract DBVal getRTAvgValue(DBValParam dbValParam, long startTime, long endTime);

    /**
     * 获取区间内tagName点 点数
     *
     * @param dbValParam
     * @param startTime
     * @param endTime
     * @return
     */
    public abstract AggCountResult getRTCountValue(DBValParam dbValParam, long startTime, long endTime);

    /**
     * 将指定时间窗口内时间序列按指定时间粒度执行降采样查询，并返回降采样结果
     *
     * @param timeGranularity 降采样粒度
     * @param dbValParam      待查询时间序列
     * @param startTime       初始时间
     * @param endTime         尾部时间
     * @return
     */
    public abstract List<DBVal> downSamplingQuery(AggFunctionType aggFunctionType, long timeGranularity,
                                                  DBValParam dbValParam, long startTime, long endTime);
}
