package com.fuzzy.common.tsaf.aggregation;

import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

public enum AggregationType {
    AVG,
    COUNT,
    SPREAD,             // 最大值和最小值差
    SUM,
    STDDEV,             // 样本标准差
    STDDEV_POP,         // 总体标准差
    VAR_POP,            // 方差

    // 选择函数
    FIRST,
    LAST,
    MAX,
    MIN,
    EXTREME,            // 最大绝对值
    MAX_TIME,
    MIN_TIME,
    ;

    public final static int arithmeticScale = 15;
    // 当计算标准差、方差时, 若窗口内仅含一个数值, 是否用0值替代计算结果
    private boolean singleValueFillZero = false;

    public void setSingleValueFillZero(boolean singleValueFillZero) {
        this.singleValueFillZero = singleValueFillZero;
    }

    // 左闭右开
    public Map<Long, List<BigDecimal>> apply(Map<Long, List<BigDecimal>> input, long startTimestamp, long duration,
                                             long sliding) {
        Map<Long, List<BigDecimal>> results = new HashMap<>();
        if (input.isEmpty()) return results;

        List<Long> sortedData = input.keySet().stream().sorted(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        }).collect(Collectors.toList());
        long windowStart = startTimestamp;
        long windowEnd = windowStart + duration;

        while (windowStart <= sortedData.get(sortedData.size() - 1)) {
            List<BigDecimal> result = computeInTimeWindow(input, sortedData, windowStart, windowEnd, this);
            if (!ObjectUtils.isEmpty(result)) results.put(windowStart, result);

            windowStart += sliding;
            windowEnd = windowStart + duration;
        }
        return results;
    }

    private List<BigDecimal> computeInTimeWindow(Map<Long, List<BigDecimal>> input, List<Long> sortedData,
                                                 long windowStart, long windowEnd, AggregationType aggregationType) {
        Map<Integer, AggregationAttribute> aggregationAttributes = new HashMap<>();
        int count = 0;

        for (int index = 0; index < sortedData.size(); index++) {
            Long timestampKey = sortedData.get(index);
            if (timestampKey >= windowEnd) break;

            List<BigDecimal> valueList = input.get(timestampKey);
            if (timestampKey >= windowStart) {
                for (int i = 0; i < valueList.size(); i++) {
                    AggregationAttribute aggregationAttribute =
                            aggregationAttributes.getOrDefault(i, new AggregationAttribute());
                    BigDecimal value = valueList.get(i);
                    aggregationAttribute.setSum(aggregationAttribute.getSum().add(value));
                    aggregationAttribute.getWindowValues().add(value);
                    aggregationAttribute.getWindowTimestamps().add(new BigDecimal(timestampKey));
                    aggregationAttribute.setMaxNum(aggregationAttribute.getMaxNum().max(value));
                    aggregationAttribute.setMinNum(aggregationAttribute.getMinNum().min(value));
                    if (aggregationAttribute.getExtreme().abs().compareTo(value.abs()) < 0 ||
                            (aggregationAttribute.getExtreme().abs().compareTo(value.abs()) == 0
                                    && value.compareTo(BigDecimal.ZERO) > 0))
                        aggregationAttribute.setExtreme(value);
                    aggregationAttributes.put(i, aggregationAttribute);
                }
                count++;
            }
        }
        if (count == 0) return null;

        // 均值、方差、标准差
        List<BigDecimal> results = new ArrayList<>();
        for (AggregationAttribute aggregationAttribute : aggregationAttributes.values()) {
            aggregationAttribute.setAvg(aggregationAttribute.getSum().divide(
                    new BigDecimal(count), arithmeticScale, RoundingMode.HALF_UP));
            for (BigDecimal value : aggregationAttribute.getWindowValues())
                aggregationAttribute.setVariance(aggregationAttribute.getVariance().add(
                        value.subtract(aggregationAttribute.getAvg()).pow(2)));
            aggregationAttribute.setVariance(aggregationAttribute.getVariance()
                    .divide(new BigDecimal(count), arithmeticScale, RoundingMode.HALF_UP));

            switch (aggregationType) {
                case COUNT:
                    results.add(new BigDecimal(count));
                    break;
                case SUM:
                    results.add(aggregationAttribute.getSum());
                    break;
                case AVG:
                    results.add(aggregationAttribute.getAvg());
                    break;
                case SPREAD:
                    results.add(aggregationAttribute.getMaxNum().subtract(aggregationAttribute.getMinNum()));
                    break;
                case STDDEV:
                    // 只有存在多个值才能够执行该聚合
                    BigDecimal stdDev = aggregationAttribute.getWindowValues().size() > 1 ?
                            sqrt(aggregationAttribute.getVariance().multiply(new BigDecimal(count)).divide(
                                            new BigDecimal(count - 1), arithmeticScale, RoundingMode.HALF_UP),
                                    arithmeticScale) : getSingleAggregationValue();
                    if (stdDev != null) results.add(stdDev);
                    break;
                case STDDEV_POP:
                    // 只有存在多个值才能够执行该聚合
                    BigDecimal stdDevPop = aggregationAttribute.getWindowValues().size() > 1 ?
                            sqrt(aggregationAttribute.getVariance(), arithmeticScale) : getSingleAggregationValue();
                    if (stdDevPop != null) results.add(stdDevPop);
                    break;
                case VAR_POP:
                    BigDecimal varPop = aggregationAttribute.getWindowValues().size() > 1 ?
                            aggregationAttribute.getVariance() : getSingleAggregationValue();
                    if (varPop != null) results.add(varPop);
                    break;
                // 选择函数
                case FIRST:
                    results.add(aggregationAttribute.getWindowValues().get(0));
                    break;
                case LAST:
                    results.add(aggregationAttribute.getWindowValues().get(
                            aggregationAttribute.getWindowValues().size() - 1));
                    break;
                case MAX:
                    results.add(aggregationAttribute.getMaxNum());
                    break;
                case MIN:
                    results.add(aggregationAttribute.getMinNum());
                    break;
                case EXTREME:
                    results.add(aggregationAttribute.getExtreme());
                    break;
                case MAX_TIME:
                    results.add(aggregationAttribute.getWindowTimestamps()
                            .get(aggregationAttribute.getWindowTimestamps().size() - 1));
                    break;
                case MIN_TIME:
                    results.add(aggregationAttribute.getWindowTimestamps().get(0));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return results;
    }

    public BigDecimal sqrt(BigDecimal value, int scale) {
        return BigDecimal.valueOf(Math.sqrt(value.doubleValue())).setScale(scale, RoundingMode.HALF_UP);
    }

    private BigDecimal getSingleAggregationValue() {
        return singleValueFillZero ? BigDecimal.ZERO : null;
    }
}
