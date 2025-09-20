package com.fuzzy.influxdb.tsaf;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.tsaf.timeseriesfunction.TimeSeriesFunction;
import com.fuzzy.influxdb.InfluxDBSchema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public enum InfluxDBTimeSeriesFunc {
    CUMULATIVE_SUM {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 累加和, 忽略 NULL 值
            // CUMULATIVE_SUM(expr)
            assert args.size() == 1;
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    BigDecimal sumValue = results.get(previousTimestamp.get()) == null ? BigDecimal.ZERO :
                            results.get(previousTimestamp.get()).get(i);
                    valList.add(sortedValue.get(i).add(sumValue));
                }
                results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    DIFFERENCE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计表中特定列与之前行的当前列有效值之差。
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
    DERIVATIVE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计表中某列数值的单位变化率
            // DERIVATIVE(expr, time_interval)
            assert args.size() == 2;
            // unit: s
            Long timeInterval = Long.parseLong(args.get(1).substring(0, args.get(1).length() - 1));
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    // unit: (ns -> s)
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    BigDecimal keyInterval = new BigDecimal((key - previousTimestamp.get()) / (1000 * 1000 * 1000));
                    valList.add(sortedValue.get(i).subtract(lastVal)
                            .divide(keyInterval, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP)
                            .multiply(BigDecimal.valueOf(timeInterval)));
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    ELAPSED {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 返回后续字段值的时间戳之间的差值。
            // ELAPSED(expr)
            // unit: ns
            assert args.size() == 2;
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
    MOVING_AVERAGE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 计算连续 k 个值的移动平均数
            // MOVING_AVERAGE(expr, k)
            // singleColumn, 1 ≤ k ≤ 1000
            assert args.size() == 2;
            int k = Integer.parseInt(args.get(1));
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            if (sortedValues.size() < k) return results;

            BigDecimal sumVal = BigDecimal.ZERO;
            // compute
            ArrayList<Map.Entry<Long, List<BigDecimal>>> entries = new ArrayList<>(sortedValues.entrySet());
            for (int i = 0; i < entries.size(); i++) {
                sumVal = sumVal.add(entries.get(i).getValue().get(0));
                // 窗口滑动
                if (i >= k)
                    sumVal = sumVal.subtract(entries.get(i - k).getValue().get(0));
                // 存储均值
                if (i >= k - 1)
                    results.put(entries.get(i).getKey(), Collections.singletonList(sumVal.divide(
                            BigDecimal.valueOf(k), DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP)));
            }
            return results;
        }
    },
    NON_NEGATIVE_DERIVATIVE {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 返回后续字段值之间的非负变化率
            // NON_NEGATIVE_DERIVATIVE(expr, time_interval)
            assert args.size() == 2;
            // unit: s
            Long timeInterval = Long.parseLong(args.get(1).substring(0, args.get(1).length() - 1));
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    // unit: (ns -> s)
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    BigDecimal keyInterval = new BigDecimal((key - previousTimestamp.get()) / (1000 * 1000 * 1000));
                    BigDecimal val = sortedValue.get(i).subtract(lastVal)
                            .divide(keyInterval, DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP)
                            .multiply(BigDecimal.valueOf(timeInterval));
                    if (val.compareTo(BigDecimal.ZERO) >= 0) valList.add(val);
                    else break;
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
            // 统计表中特定列与之前行的当前列有效值之差。
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
                    BigDecimal val = sortedValue.get(i).subtract(lastVal);
                    if (val.compareTo(BigDecimal.ZERO) >= 0) valList.add(val);
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
        return getInfluxDBTimeSeriesFunc(timeSeriesFunction).apply(input, timeSeriesFunction.getArgs());
    }

    public static InfluxDBTimeSeriesFunc getInfluxDBTimeSeriesFunc(TimeSeriesFunction timeSeriesFunction) {
        for (InfluxDBTimeSeriesFunc value : InfluxDBTimeSeriesFunc.values()) {
            if (value.name().equalsIgnoreCase(timeSeriesFunction.getFunctionType())) return value;
        }
        throw new IllegalArgumentException();
    }

    public static TimeSeriesFunction getRandomFunction(List<InfluxDBSchema.InfluxDBColumn> columns, Randomly randomly) {
        // TODO
//        List<InfluxDBTimeSeriesFunc> allFunctions = Arrays.stream(values()).collect(Collectors.toList());
        List<InfluxDBTimeSeriesFunc> allFunctions = Collections.singletonList(DIFFERENCE);

        InfluxDBTimeSeriesFunc functionType = Randomly.fromList(allFunctions);
        List<String> args = getRandomArgsForFunction(columns, functionType, randomly);
        return new TimeSeriesFunction(functionType.name(), args);
    }

    private static List<String> getRandomArgsForFunction(List<InfluxDBSchema.InfluxDBColumn> columns,
                                                         InfluxDBTimeSeriesFunc functionType, Randomly randomly) {
        List<String> args = new ArrayList<>();
        args.add(Randomly.fromList(columns).getName());
        switch (functionType) {
            case CUMULATIVE_SUM:
            case DIFFERENCE:
            case NON_NEGATIVE_DIFFERENCE:
                break;
            case DERIVATIVE:
            case NON_NEGATIVE_DERIVATIVE:
                args.add(String.format("%ss", randomly.getInteger(1, 1000)));
                break;
            case ELAPSED:
                args.add("1ns");
                break;
            case MOVING_AVERAGE:
                args.add(String.format("%d", randomly.getInteger(1, 1001)));
                break;
            default:
                throw new AssertionError();
        }
        return args;
    }
}
