package com.fuzzy.common.tsaf.aggregation;

import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Data
public class AggregationAttribute {
    private BigDecimal sum = BigDecimal.ZERO;
    private BigDecimal maxNum = new BigDecimal(Long.MIN_VALUE);
    private BigDecimal minNum = new BigDecimal(Long.MAX_VALUE);
    // 最大绝对值(若正值和负值的最大绝对值相等, 则返回正值)
    private BigDecimal extreme = BigDecimal.ZERO;
    private List<BigDecimal> windowValues = new ArrayList<>();
    private List<BigDecimal> windowTimestamps = new ArrayList<>();
    private BigDecimal avg = BigDecimal.ZERO;
    private BigDecimal variance = BigDecimal.ZERO;
}
