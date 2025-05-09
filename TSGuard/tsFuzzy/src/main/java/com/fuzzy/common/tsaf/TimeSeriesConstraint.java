package com.fuzzy.common.tsaf;

import lombok.Data;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class TimeSeriesConstraint {

    // 全部转为Y轴过滤
    private String timeSeriesName;
    private List<BigDecimal> notEqualValues = new ArrayList<>();
    private List<RangeConstraint> rangeConstraints = new ArrayList<>();

    public TimeSeriesConstraint(String timeSeriesName) {
        this.timeSeriesName = timeSeriesName;
        // 默认全局范围 -> 若某次交集后区间为空, 说明返回最终结果为空集
        this.rangeConstraints.add(new RangeConstraint());
    }

    public TimeSeriesConstraint(String timeSeriesName, RangeConstraint rangeConstraint) {
        this.timeSeriesName = timeSeriesName;
        this.rangeConstraints.add(rangeConstraint);
    }

    // 结果为空集 -> rangeConstraints范围为空
    // 只有取交集时才会导致结果为空集
    public boolean isEmpty() {
        return rangeConstraints.isEmpty();
    }

    public TimeSeriesConstraint intersects(TimeSeriesConstraint constraint) {
        TimeSeriesConstraint intersectRes = new TimeSeriesConstraint(this.timeSeriesName);
        // 多段区间求交集
        List<RangeConstraint> res = new ArrayList<>();
        this.rangeConstraints.forEach(rangeConstraint -> {
            for (int i = 0; i < constraint.getRangeConstraints().size(); i++) {
                RangeConstraint tempRangeConstraint = rangeConstraint.intersects(
                        constraint.getRangeConstraints().get(i));
                if (tempRangeConstraint != null) res.add(tempRangeConstraint);
            }
        });
        intersectRes.setRangeConstraints(res);
        // 取不等点交集
        this.notEqualValues.addAll(constraint.getNotEqualValues());
        intersectRes.setNotEqualValues(new ArrayList<>(this.notEqualValues));
        // 合并覆盖取最大区间
        intersectRes.merge();
        return intersectRes;
    }

    public TimeSeriesConstraint union(TimeSeriesConstraint constraint, BigDecimal tolerance) {
        TimeSeriesConstraint unionRes = new TimeSeriesConstraint(this.timeSeriesName);
        // 剔除不等点
        List<BigDecimal> notEqualValues = new ArrayList<>();
        notEqualValues.addAll(this.notEqualValues.stream().filter(value -> !constraint.valueInRange(value, tolerance))
                .collect(Collectors.toList()));
        notEqualValues.addAll(constraint.getNotEqualValues().stream().filter(value -> !valueInRange(value, tolerance))
                .collect(Collectors.toList()));
        unionRes.setNotEqualValues(notEqualValues);

        // union合并覆盖区间
        List<RangeConstraint> unionRangeConstraint = new ArrayList<>();
        unionRangeConstraint.addAll(this.rangeConstraints);
        unionRangeConstraint.addAll(constraint.getRangeConstraints());
        unionRes.setRangeConstraints(unionRangeConstraint);
        unionRes.merge();
        return unionRes;
    }

    public TimeSeriesConstraint complement() {
        TimeSeriesConstraint complementRes = new TimeSeriesConstraint(this.timeSeriesName);
        List<BigDecimal> complementNotEqualValues = new ArrayList<>();
        // 集合取反集
        List<RangeConstraint> complementConstraints = new ArrayList<>();
        BigDecimal lower = new BigDecimal(Long.MIN_VALUE);
        BigDecimal upper = new BigDecimal(Long.MAX_VALUE);
        for (RangeConstraint rangeConstraint : this.rangeConstraints) {
            if (lower.compareTo(upper) == 0) break;
            BigDecimal rangeLeft = rangeConstraint.getGreaterEqualValue();
            BigDecimal rangeRight = rangeConstraint.getLessEqualValue();

            // (lower, rangeLeft)
            if (rangeLeft.compareTo(lower) > 0) {
                complementConstraints.add(new RangeConstraint(lower, rangeLeft));
                complementNotEqualValues.add(lower);
                complementNotEqualValues.add(rangeLeft);
            }

            lower = rangeRight;
        }
        // 当前至无穷大
        if (lower.compareTo(upper) < 0) {
            complementConstraints.add(new RangeConstraint(lower, upper));
            complementNotEqualValues.add(lower);
        }
        complementRes.setRangeConstraints(complementConstraints);
        // 移除不等值点
        for (BigDecimal notEqualValue : this.notEqualValues) {
            boolean isRemove = false;
            Iterator<BigDecimal> iterator = complementNotEqualValues.iterator();
            while (iterator.hasNext()) {
                // 等值 -> 移除
                if (iterator.next().compareTo(notEqualValue) == 0) {
                    iterator.remove();
                    isRemove = true;
                }
            }
            // 该点未处理: 添加为分段函数
            if (!isRemove) complementConstraints.add(new RangeConstraint(notEqualValue, notEqualValue));
        }
        complementRes.setNotEqualValues(complementNotEqualValues);
        // 合并范围
        complementRes.merge();
        return complementRes;
    }

    public void merge() {
        if (rangeConstraints.isEmpty()) return;

        rangeConstraints.sort(new Comparator<RangeConstraint>() {
            @Override
            public int compare(RangeConstraint o1, RangeConstraint o2) {
                return o1.getGreaterEqualValue().compareTo(o2.getGreaterEqualValue());
            }
        });

        List<RangeConstraint> mergeRes = new ArrayList<>();
        BigDecimal start = rangeConstraints.get(0).getGreaterEqualValue();
        BigDecimal end = rangeConstraints.get(0).getLessEqualValue();
        for (int i = 1; i < rangeConstraints.size(); i++) {
            if (rangeConstraints.get(i).getGreaterEqualValue().compareTo(end) <= 0)
                end = end.max(rangeConstraints.get(i).getLessEqualValue());
            else {
                mergeRes.add(new RangeConstraint(start, end));
                start = rangeConstraints.get(i).getGreaterEqualValue();
                end = rangeConstraints.get(i).getLessEqualValue();
            }
        }
        // the last element
        mergeRes.add(new RangeConstraint(start, end));
        this.rangeConstraints = mergeRes;

        // 存在覆盖区间, 其不等值点才有意义
        if (this.rangeConstraints.isEmpty()) {
            this.notEqualValues.clear();
            return;
        }

        // 过滤掉不在范围内的不等点
        this.notEqualValues = this.notEqualValues.stream().filter(value -> {
            for (int i = 0; i < this.rangeConstraints.size(); i++) {
                BigDecimal greaterEqualValue = this.rangeConstraints.get(i).getGreaterEqualValue();
                BigDecimal lessEqualValue = this.rangeConstraints.get(i).getLessEqualValue();
                if (value.compareTo(greaterEqualValue) >= 0 && value.compareTo(lessEqualValue) <= 0) return true;
            }
            return false;
        }).collect(Collectors.toList());

        // 去重(转为字符串)
        this.notEqualValues = this.notEqualValues.stream().map(BigDecimal::toPlainString).collect(Collectors.toSet())
                .stream().map(BigDecimal::new).collect(Collectors.toList());
    }

    public void addNotEqualValue(BigDecimal bigDecimal) {
        this.notEqualValues.add(bigDecimal.stripTrailingZeros());
    }

    public void addEqualValue(BigDecimal bigDecimal) {
        this.rangeConstraints.add(new RangeConstraint(bigDecimal, bigDecimal));
        this.merge();
    }

    public boolean valueInRange(BigDecimal value, BigDecimal tolerance) {
        // 不具备有效值范围
        if (this.rangeConstraints.isEmpty()) return false;

        // 判断不等点是否和该值重合, 重合返回false, 即不在区间内
        for (BigDecimal notEqualValue : this.notEqualValues) {
            if (compareBigDecimal(notEqualValue, value, tolerance) == 0) return false;
        }

        // 在包含覆盖区间内
        for (int i = 0; i < this.rangeConstraints.size(); i++) {
            BigDecimal greaterEqualValue = this.rangeConstraints.get(i).getGreaterEqualValue();
            BigDecimal lessEqualValue = this.rangeConstraints.get(i).getLessEqualValue();
            // 等值: 在指定精度范围内均有效
            if (compareBigDecimal(value, greaterEqualValue, tolerance) >= 0
                    && compareBigDecimal(value, lessEqualValue, tolerance) <= 0)
                return true;
        }
        return false;
    }

    public int compareBigDecimal(BigDecimal left, BigDecimal right, BigDecimal tolerance) {
        // 使用精度阈值判断相等
        BigDecimal difference = left.subtract(right).abs();
        if (difference.compareTo(tolerance) <= 0) return 0;

        // 0 = 0
        if (left.compareTo(BigDecimal.ZERO) == 0 && right.compareTo(BigDecimal.ZERO) == 0) return 0;

        String[] leftVal = convertToScientificNotation(left).split("E");
        String[] rightVal = convertToScientificNotation(right).split("E");
        boolean leftIsNegative = leftVal[0].charAt(0) == '-';
        boolean rightIsNegative = rightVal[0].charAt(0) == '-';

        // -0 = 0
        if (leftIsNegative ^ rightIsNegative && left.compareTo(BigDecimal.ZERO) == 0
                && right.compareTo(BigDecimal.ZERO) == 0) return 0;
        else if (leftIsNegative ^ rightIsNegative) return leftIsNegative ? -1 : 1;

        // 存在0值
        if (left.compareTo(BigDecimal.ZERO) == 0) return rightIsNegative ? 1 : -1;
        else if (right.compareTo(BigDecimal.ZERO) == 0) return leftIsNegative ? -1 : 1;

        // 比较指数
        int exponentComparisonRes = new BigDecimal(leftVal[1]).compareTo(new BigDecimal(rightVal[1]));
        if (exponentComparisonRes > 0) return leftIsNegative ? -1 : 1;
        else if (exponentComparisonRes < 0) return leftIsNegative ? 1 : -1;
        else return new BigDecimal(leftVal[0]).compareTo(new BigDecimal(rightVal[0]));
    }

    private String convertToScientificNotation(BigDecimal number) {
        DecimalFormat scientificFormat = new DecimalFormat(".###############E0");
        return scientificFormat.format(number);
    }

    public TimeSeriesConstraint transConstraintByEquations(Equations equations) {
        TimeSeriesConstraint transConstraintRes = equations.transformTimeSeriesConstraint(this);
        transConstraintRes.merge();
        return transConstraintRes;
    }
}
