package com.fuzzy.TDengine.tsaf.enu;

import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.ast.TDengineConstant;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.tsaf.timeseriesfunction.TimeSeriesFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public enum TDengineTimeSeriesFunc {
    // single-group
//    IRATE(true),
//    TWA(true),
    CSUM(false) {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 累加和, 忽略 NULL 值
            // CSUM(expr)
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
    DIFF(false) {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计表中特定列与之前行的当前列有效值之差。
            // DIFF(expr [, ignore_option])
            // 0 表示不忽略(diff结果)负值不忽略 null 值
            // 1 表示(diff结果)负值作为 null 值
            // 2 表示不忽略(diff结果)负值但忽略 null 值
            // 3 表示忽略(diff结果)负值且忽略 null 值
            assert args.size() == 2;
            int ignoreOption = Integer.parseInt(args.get(1));
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            sortedValues.forEach((key, sortedValue) -> {
                List<BigDecimal> valList = new ArrayList<>();
                for (int i = 0; i < sortedValue.size(); i++) {
                    if (previousTimestamp.get() == 0) break;
                    else if ((sortedValue.get(i) == null && (ignoreOption == 2 || ignoreOption == 3))
                            || (sortedValue.get(i).compareTo(BigDecimal.ZERO) < 0 && ignoreOption == 3)
                            || (sortedValue.get(i).compareTo(BigDecimal.ZERO) < 0 && ignoreOption == 1)) continue;
                    BigDecimal lastVal = sortedValues.get(previousTimestamp.get()).get(i);
                    valList.add(sortedValue.get(i).subtract(lastVal));
                }
                if (!valList.isEmpty()) results.put(key, valList);
                previousTimestamp.set(key);
            });
            return results;
        }
    },
    DERIVATIVE(true) {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 统计表中某列数值的单位变化率 -> 仅支持一列
            // DERIVATIVE(expr, time_interval, ignore_negative)
            assert args.size() == 3;
            Long timeInterval = Long.parseLong(args.get(1).substring(0, args.get(1).length() - 1));
            boolean ignoreNegative = args.get(2).equalsIgnoreCase("1");
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
                    BigDecimal keyInterval = new BigDecimal((key - previousTimestamp.get()) / 1000);
                    // 忽略负值
                    if (ignoreNegative && sortedValue.get(i).subtract(lastVal).compareTo(BigDecimal.ZERO) < 0) break;
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
    MAVG(true) {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 计算连续 k 个值的移动平均数
            // MAVG(expr, k)
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
    STATECOUNT(true) {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 返回满足某个条件的连续记录的个数
            // STATECOUNT(expr, oper, val)
            // singleColumn
            assert args.size() == 3;
            TDengineTimeSeriesFuncOp op = TDengineTimeSeriesFuncOp.valueOf(
                    args.get(1).substring(1, args.get(1).length() - 1));
            BigDecimal val = new BigDecimal(args.get(2));
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();

            BigDecimal count = BigDecimal.ZERO;
            // compute
            ArrayList<Map.Entry<Long, List<BigDecimal>>> entries = new ArrayList<>(sortedValues.entrySet());
            for (int i = 0; i < entries.size(); i++) {
                if (op.compare(entries.get(i).getValue().get(0), val)) {
                    if (count.equals(BigDecimal.ONE.negate())) count = BigDecimal.ONE;
                    else count = count.add(BigDecimal.ONE);
                } else count = BigDecimal.ONE.negate();
                results.put(entries.get(i).getKey(), Collections.singletonList(count));
            }
            return results;
        }
    },
    STATEDURATION(true) {
        @Override
        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
            // 返回满足某个条件的连续记录的时间长度
            // STATEDURATION(expr, oper, val)
            // singleColumn
            assert args.size() == 3;
            TDengineTimeSeriesFuncOp op = TDengineTimeSeriesFuncOp.valueOf(
                    args.get(1).substring(1, args.get(1).length() - 1));
            BigDecimal val = new BigDecimal(args.get(2));
            // sort
            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
            Map<Long, List<BigDecimal>> results = new HashMap<>();

            BigDecimal count = BigDecimal.ONE.negate();
            AtomicLong previousTimestamp = new AtomicLong(0);
            // compute
            ArrayList<Map.Entry<Long, List<BigDecimal>>> entries = new ArrayList<>(sortedValues.entrySet());
            for (int i = 0; i < entries.size(); i++) {
                Long timestamp = entries.get(i).getKey();
                if (op.compare(entries.get(i).getValue().get(0), val)) {
                    if (count.equals(BigDecimal.ONE.negate())) count = BigDecimal.ZERO;
                    else count = count.add(BigDecimal.valueOf(timestamp - previousTimestamp.get()));
                } else count = BigDecimal.ONE.negate();
                results.put(timestamp, Collections.singletonList(count));
                previousTimestamp.set(timestamp);
            }
            return results;
        }
    },
    //    IRATE(false) {
//        @Override
//        public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args) {
//            // 计算瞬时增长率
//            // IRATE(expr)
//            assert args.size() == 1;
//            // sort
//            TreeMap<Long, List<BigDecimal>> sortedValues = new TreeMap<>(input);
//            Map<Long, List<BigDecimal>> results = new HashMap<>();
//            // compute
//            ArrayList<List<BigDecimal>> values = new ArrayList<>(sortedValues.values());
//            if (values.size() == 0) return results;
//            else if (values.size() == 1) {
//                results.put(0L, Collections.singletonList(BigDecimal.ZERO));
//            } else {
//                BigDecimal lastVal = values.get(values.size() - 1).get(0);
//                BigDecimal penultimateVal = values.get(values.size() - 2).get(0);
//                List<Long> timestampList = new ArrayList<>(sortedValues.keySet());
//                Long lastTimestamp = timestampList.get(timestampList.size() - 1);
//                Long penultimateTimestamp = timestampList.get(timestampList.size() - 1);
//                BigDecimal timestampInterval = new BigDecimal((penultimateTimestamp - lastTimestamp) / 1000);
//                BigDecimal iRate;
//                if (lastVal.compareTo(BigDecimal.ZERO) < 0)
//                    iRate = lastVal.divide(timestampInterval, DoubleArithmeticPrecisionConstant.scale,
//                            RoundingMode.HALF_UP);
//                else
//                    iRate = lastVal.subtract(penultimateVal).divide(timestampInterval,
//                            DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP);
//                results.put(0L, Collections.singletonList(iRate));
//            }
//            return results;
//        }
//    },
    ;

    boolean singleColumn;

    TDengineTimeSeriesFunc(boolean singleColumn) {
        this.singleColumn = singleColumn;
    }

    public boolean isSingleColumn() {
        return singleColumn;
    }

    public abstract Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, List<String> args);

    public static Map<Long, List<BigDecimal>> apply(TimeSeriesFunction timeSeriesFunction,
                                                    Map<Long, List<BigDecimal>> input) {
        return getTDengineTimeSeriesFunc(timeSeriesFunction).apply(input, timeSeriesFunction.getArgs());
    }

    public static TDengineTimeSeriesFunc getTDengineTimeSeriesFunc(TimeSeriesFunction timeSeriesFunction) {
        for (TDengineTimeSeriesFunc value : TDengineTimeSeriesFunc.values()) {
            if (value.name().equalsIgnoreCase(timeSeriesFunction.getFunctionType())) return value;
        }
        throw new IllegalArgumentException();
    }

    public static TimeSeriesFunction getRandomFunction(List<TDengineSchema.TDengineColumn> columns, Randomly randomly) {
        List<TDengineTimeSeriesFunc> multiColumnFunctions = Arrays.stream(values())
                .filter(func -> !func.isSingleColumn()).collect(Collectors.toList());
        List<TDengineTimeSeriesFunc> allFunctions = Arrays.stream(values()).collect(Collectors.toList());

        TDengineTimeSeriesFunc functionType;
        if (columns.size() == 1) functionType = Randomly.fromList(allFunctions);
        else functionType = Randomly.fromList(multiColumnFunctions);

        List<String> args = getRandomArgsForFunction(columns, functionType, randomly);
        return new TimeSeriesFunction(functionType.name(), args);
    }

    private static List<String> getRandomArgsForFunction(List<TDengineSchema.TDengineColumn> columns,
                                                         TDengineTimeSeriesFunc functionType, Randomly randomly) {
        List<String> args = new ArrayList<>();
        args.add(Randomly.fromList(columns).getName());
        switch (functionType) {
            case CSUM:
//            case IRATE:
                break;
            case DERIVATIVE:
                args.add(String.format("%ss", randomly.getInteger(1, 1000)));
                args.add(String.format("%d", Randomly.fromOptions(0, 1)));
                break;
            case DIFF:
                args.add(String.format("%d", Randomly.fromOptions(2)));
                break;
            case MAVG:
                args.add(String.format("%d", randomly.getInteger(1, 1001)));
                break;
            case STATECOUNT:
            case STATEDURATION:
                args.add(String.format("\"%s\"", Randomly.fromOptions(TDengineTimeSeriesFuncOp.values())));
                double stateDurationVal = BigDecimal.valueOf((double) randomly.getInteger() / randomly.getInteger()).setScale(
                        TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                args.add(String.valueOf(stateDurationVal));
                break;
            default:
                throw new AssertionError();
        }
        return args;
    }
}
