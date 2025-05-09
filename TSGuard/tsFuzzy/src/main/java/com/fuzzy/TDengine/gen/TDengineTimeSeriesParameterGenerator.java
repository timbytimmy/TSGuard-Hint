package com.fuzzy.TDengine.gen;

import cn.hutool.core.util.ObjectUtil;
import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.common.schema.TimeSeriesTemplate;

import java.util.ArrayList;
import java.util.List;

public class TDengineTimeSeriesParameterGenerator {

    private final TDengineSchema.TDengineTable table;
    private final TDengineGlobalState globalState;
    private List<Long> timestamps;

    public TDengineTimeSeriesParameterGenerator(TDengineSchema.TDengineTable table, TDengineGlobalState globalState) {
        this.table = table;
        this.globalState = globalState;
        this.timestamps = genAllTimestampsForTable(this.table.getName());
    }

    public List<Object> genTemplateWhereParams(TimeSeriesTemplate<TDengineSchema.TDengineDataType> template) {
        Randomly randomly = globalState.getRandomly();

        // 依据类型随机生成参数值
        List<Object> values = new ArrayList<>();
        for (TDengineSchema.TDengineDataType parameterType : template.getParameterTypes()) {
            switch (parameterType) {
                case INT:
                    values.add(randomly.getInteger());
                    break;
                case UINT:
                    values.add(randomly.getInteger());
                    break;
                case BIGINT:
                case UBIGINT:
                    values.add(randomly.getLong());
                    break;
                case BINARY:
                case VARCHAR:
                    values.add(randomly.getString().replace("\\", "").replace("\n", ""));
                    break;
                case BOOL:
                    values.add(Randomly.getBoolean());
                    break;
                case DOUBLE:
                    values.add(randomly.getInfiniteDouble());
                    break;
                case TIMESTAMP:
                    values.add(Randomly.fromList(this.timestamps));
                    break;
                default:
                    throw new AssertionError(this);
            }
        }

        // 校验参数 -> 时间戳
        // 多时间参数校验 - 大小交换
        if (template.getParameterTypes().size() > 1
                && template.getParameterTypes().get(0) == template.getParameterTypes().get(1)
                && ObjectUtil.equals(template.getParameterTypes().get(0), TDengineSchema.TDengineDataType.TIMESTAMP)) {
            if ((long) values.get(0) > (long) values.get(1)) {
                long tmp = (long) values.get(0);
                values.set(0, values.get(1));
                values.set(1, tmp);
            }
        }

        return values;
    }

    private List<Long> genAllTimestampsForTable(String tableName) {
        return genTimestampsInRange(globalState.getOptions().getStartTimestampOfTSData(),
                TDengineInsertGenerator.getLastTimestamp(globalState.getDatabaseName(), tableName),
                globalState.getOptions().getSamplingFrequency());
    }

    public Long genInterval() {
        int factor = globalState.getRandomly().getInteger(10, 100);
        return globalState.getOptions().getSamplingFrequency() * factor;
    }

    public List<Long> genTimestampsInRange(Long startTime, Long endTime, Long samplingFrequency) {
        // 指定时间范围内进行时间戳采样, 采取左闭右闭
        List<Long> timestamps = new ArrayList<>();
        for (long i = startTime; i <= endTime; i += samplingFrequency) timestamps.add(i);
        return timestamps;
    }

    public List<Long> genTimestampsInRangeSplitByInterval(Long startTime, Long endTime, Long interval) {
        // 将时间按照窗口进行切分, 从最初插入数据点开始
        List<Long> timestamps = new ArrayList<>();
        timestamps.add(startTime);
        for (long i = globalState.getOptions().getStartTimestampOfTSData(); i < endTime; i += interval) {
            // 该段窗口满足条件
            if (i > startTime) timestamps.add(i);
        }
        timestamps.add(endTime);
        return timestamps;
    }

    public TDengineSchema.TDengineTable getTable() {
        return table;
    }

    public TDengineGlobalState getGlobalState() {
        return globalState;
    }
}
