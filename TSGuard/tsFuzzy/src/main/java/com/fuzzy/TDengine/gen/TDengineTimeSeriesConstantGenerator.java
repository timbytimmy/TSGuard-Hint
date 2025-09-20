package com.fuzzy.TDengine.gen;

import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.ast.TDengineCastOperation;
import com.fuzzy.TDengine.ast.TDengineConstant;
import com.fuzzy.common.tsaf.aggregation.AggregationType;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class TDengineTimeSeriesConstantGenerator {

    private final TDengineGlobalState state;

    public TDengineTimeSeriesConstantGenerator(TDengineGlobalState state) {
        this.state = state;
    }

    public TDengineConstant generateConstantByTimestamp(long timestamp) {
        return createConstantByValue(generateValueByTimestamp(timestamp),
                // TODO 返回类型
                Randomly.fromOptions(TDengineSchema.TDengineDataType.valuesTSAF()));
    }

    public TDengineConstant generateConstantByTimestampAndId(long timestamp, int deviceId) {
        return createConstantByValue(generateValueByTimestampAndId(timestamp, deviceId),
                Randomly.fromOptions(TDengineSchema.TDengineDataType.valuesTSAF()));
    }

    private TDengineConstant createConstantByValue(String value, TDengineSchema.TDengineDataType dataType) {
        TDengineConstant stringConstant = TDengineConstant.createStringConstant(String.valueOf(value));
        return stringConstant.castAs(TDengineCastOperation.CastType.TDengineDataTypeToCastType(dataType));
    }

    private String generateValueByTimestamp(long timestamp) {
        long value = (timestamp - state.getOptions().getStartTimestampOfTSData()) /
                state.getOptions().getSamplingFrequency();
        return String.valueOf(value);
    }

    private String generateValueByTimestampAndId(long timestamp, int deviceId) {
        long value = (timestamp - state.getOptions().getStartTimestampOfTSData()) /
                state.getOptions().getSamplingFrequency()/*+ deviceId*/;
        return String.valueOf(value);
    }

    public TDengineConstant generateAggregationResult(Long startTimestamp, Long endTimestamp, int deviceId,
                                                      AggregationType aggregationType) {
        BigDecimal sum = new BigDecimal(0);
        long count = 0;
        BigDecimal maxVal = new BigDecimal(Long.MIN_VALUE);
        BigDecimal minVal = new BigDecimal(Long.MAX_VALUE);
        for (long timestamp = startTimestamp; timestamp < endTimestamp;
             timestamp += state.getOptions().getSamplingFrequency()) {
            BigDecimal val = new BigDecimal(generateValueByTimestampAndId(timestamp, deviceId));
            sum = sum.add(val);
            maxVal = maxVal.max(val);
            minVal = minVal.min(val);
            count++;
        }

        switch (aggregationType) {
            case AVG:
                return TDengineConstant.createDoubleConstant(
                        sum.divide(BigDecimal.valueOf(count),
                                TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue());
            case SUM:
                return TDengineConstant.createDoubleConstant(
                        sum.setScale(TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue());
            case COUNT:
                return TDengineConstant.createInt32Constant(count);
            case SPREAD:
                return TDengineConstant.createDoubleConstant(maxVal.subtract(minVal)
                        .setScale(TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue());
            default:
                throw new AssertionError();
        }
    }
}
