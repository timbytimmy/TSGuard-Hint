package com.fuzzy.iotdb.tsaf;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.tsaf.timeseriesfunction.TimeSeriesFunction;
import com.fuzzy.iotdb.IotDBSchema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public enum IotDBTimeSeriesFunc {
    TIME_DIFFERENCE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 某数据点的时间戳与前一数据点时间戳的差
            // TIME_DIFFERENCE(expr)
            assert args.size() == 1;
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    BigDecimal lastTimestamp = BigDecimal.valueOf(previousTimestamp.get());
                    valList.add(BigDecimal.valueOf(key).subtract(lastTimestamp));
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    DIFFERENCE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计序列中某数据点的值与前一数据点的值的差
            // DIFFERENCE(expr)
            assert args.size() == 1;
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    valList.add(sortedValue.get(i).subtract(lastVal));
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    NON_NEGATIVE_DIFFERENCE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计序列中某数据点的值与前一数据点的值的差的绝对值
            // NON_NEGATIVE_DIFFERENCE(expr)
            assert args.size() == 1;
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    valList.add(sortedValue.get(i).subtract(lastVal).abs());
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    DERIVATIVE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计序列中某数据点相对于前一数据点的变化率
            // DERIVATIVE(expr)
            assert args.size() == 1;
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    BigDecimal timeInterval = BigDecimal.valueOf(key - previousTimestamp.get());
                    valList.add(sortedValue.get(i).subtract(lastVal).divide(timeInterval,
                            DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    NON_NEGATIVE_DERIVATIVE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计序列中某数据点相对于前一数据点的变化率
            // DERIVATIVE(expr)
            assert args.size() == 1;
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    BigDecimal timeInterval = BigDecimal.valueOf(key - previousTimestamp.get());
                    valList.add(sortedValue.get(i).subtract(lastVal).divide(timeInterval,
                            DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP).abs());
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    DIFF {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计序列中某数据点的值与前一数据点的值的差
            // DIFF(expr, ignoreNull)
            assert args.size() == 2;
            boolean ignoreNull = args.get(1).contains("true");
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    valList.add(sortedValue.get(i).subtract(lastVal));
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    ;

    public abstract Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args);

    public static Map<Long, List<BigDecimal>> apply(TimeSeriesFunction timeSeriesFunction,
                                                    Map<Long, List<BigDecimal>> input) {
        return getIotDBTimeSeriesFunc(timeSeriesFunction).apply(input, timeSeriesFunction.getArgs());
    }

    public static IotDBTimeSeriesFunc getIotDBTimeSeriesFunc(TimeSeriesFunction timeSeriesFunction) {
        for (IotDBTimeSeriesFunc value : IotDBTimeSeriesFunc.values()) {
            if (value.name().equalsIgnoreCase(timeSeriesFunction.getFunctionType())) return value;
        }
        throw new IllegalArgumentException();
    }

    public static TimeSeriesFunction getRandomFunction(List<IotDBSchema.IotDBColumn> columns, Randomly randomly) {
        List<IotDBTimeSeriesFunc> allFunctions = Arrays.stream(values()).collect(Collectors.toList());

        IotDBTimeSeriesFunc functionType = Randomly.fromList(allFunctions);
        List<String> args = getRandomArgsForFunction(columns, functionType, randomly);
        return new TimeSeriesFunction(functionType.name(), args);
    }

    private static List<String> getRandomArgsForFunction(List<IotDBSchema.IotDBColumn> columns,
                                                         IotDBTimeSeriesFunc functionType, Randomly randomly) {
        List<String> args = new ArrayList<>();
        args.add(Randomly.fromList(columns).getName());
        switch (functionType) {
            case TIME_DIFFERENCE:
            case DIFFERENCE:
            case NON_NEGATIVE_DIFFERENCE:
            case DERIVATIVE:
            case NON_NEGATIVE_DERIVATIVE:
                break;
            case DIFF:
                boolean ignoreNull = Randomly.getBoolean();
                args.add(String.format("'ignoreNull'='%s'", ignoreNull));
                break;
            default:
                throw new AssertionError();
        }
        return args;
    }
}
