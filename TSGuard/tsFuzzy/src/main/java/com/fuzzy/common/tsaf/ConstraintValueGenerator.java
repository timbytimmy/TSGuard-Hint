package com.fuzzy.common.tsaf;

import com.fuzzy.common.constant.GlobalConstant;

public class ConstraintValueGenerator {

    public static ConstraintValue.TimeSeriesValue genTimeSeries(String timeSeriesName) {
        if (timeSeriesName.contains(GlobalConstant.TIME_FIELD_NAME))
            return new ConstraintValue.TimeSeriesValue(timeSeriesName, true);
        else return new ConstraintValue.TimeSeriesValue(timeSeriesName, false);
    }

    public static ConstraintValue.TimeSeriesValue genTimeSeries(String timeSeriesName, ConstraintValue factor) {
        if (timeSeriesName.contains(GlobalConstant.TIME_FIELD_NAME))
            return new ConstraintValue.TimeSeriesValue(timeSeriesName, true, factor);
        else return new ConstraintValue.TimeSeriesValue(timeSeriesName, false, factor);
    }

    public static ConstraintValue.ConstantValue genConstant(String constantValue) {
        return new ConstraintValue.ConstantValue(constantValue);
    }

    public static ConstraintValue genOperator(String op) {
        return new ConstraintValue.OperatorValue(op);
    }

    public static ConstraintValue genConstraint(ConstraintValue.TimeSeriesValue timeSeriesValue,
                                                TimeSeriesConstraint timeSeriesConstraint) {
        // 当运算结果为BOOL值时, 生成列约束
        // 所有约束条件生成均针对于Y轴(X轴会转为Y轴)
        // 增加Y轴名称, 某个具体的ConstraintValue归属于某个TimeSeriesValue
        return new ConstraintValue.TimeSeriesConstraintValue(timeSeriesValue, timeSeriesConstraint);
    }

    public static ConstraintValue.TimeSeriesValue genBaseTimeSeries() {
        return new ConstraintValue.TimeSeriesValue(GlobalConstant.BASE_TIME_SERIES_NAME, false);
    }

    public static ConstraintValue genTrueConstraint() {
        return new ConstraintValue.TimeSeriesConstraintValue(genTimeSeries(GlobalConstant.BASE_TIME_SERIES_NAME),
                new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME));
    }

    public static ConstraintValue genFalseConstraint() {
        TimeSeriesConstraint falseTimeSeriesConstraint = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME);
        falseTimeSeriesConstraint.getRangeConstraints().clear();
        return new ConstraintValue.TimeSeriesConstraintValue(genTimeSeries(GlobalConstant.BASE_TIME_SERIES_NAME),
                falseTimeSeriesConstraint);
    }

}
