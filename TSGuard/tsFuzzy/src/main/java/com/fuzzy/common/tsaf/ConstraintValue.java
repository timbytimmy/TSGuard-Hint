package com.fuzzy.common.tsaf;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
@Slf4j
public abstract class ConstraintValue {
    public static final String SPLIT_DELIMITER = "__";
    public static final String RECIPROCAL = "reciprocal";

    public boolean isTimeSeries() {
        return false;
    }

    public boolean isTimeField() {
        return false;
    }

    public boolean isConstant() {
        return false;
    }

    public boolean isOperator() {
        return false;
    }

    public boolean isTimeSeriesConstraint() {
        return false;
    }

    public boolean valueEqualZero() {
        return false;
    }

    public static class TimeSeriesValue extends ConstraintValue {
        private final String timeSeriesName;
        private final boolean isTimeField;
        // 列表达式缩放因子、截距
        // 任何算术运算计算后列格式: factor * time series + intercept
        private ConstraintValue factor;
        private ConstraintValue intercept;
        private boolean isReciprocalVal;

        public TimeSeriesValue(String timeSeriesName, boolean isTimeField) {
            this(timeSeriesName, isTimeField, createConstantConstraintValue("1"), createConstantConstraintValue("0"));
        }

        public TimeSeriesValue(String timeSeriesName, boolean isTimeField, ConstraintValue factor) {
            this(timeSeriesName, isTimeField, factor, createConstantConstraintValue("0"));
        }

        public TimeSeriesValue(String timeSeriesName, boolean isTimeField, ConstraintValue factor, ConstraintValue intercept) {
            this.timeSeriesName = timeSeriesName;
            this.isTimeField = isTimeField;
            // TODO 系数因子和截距仅支持常量
            assert factor instanceof ConstantValue;
            assert intercept instanceof ConstantValue;
            this.factor = factor;
            this.intercept = intercept;
        }

        @Override
        public boolean isTimeSeries() {
            return true;
        }

        @Override
        public boolean isTimeField() {
            return isTimeField;
        }

        @Override
        public String getTimeSeriesName() {
//            if (!isTimeField && isReciprocalVal)
//                return String.format("%s%s%s", timeSeriesName, SPLIT_DELIMITER, isReciprocalVal);
            return timeSeriesName;
        }

        @Override
        public TimeSeriesValue getTimeSeriesValue() {
            return this;
        }

        @Override
        public TimeSeriesValue negate() {
            return new TimeSeriesValue(this.timeSeriesName, this.isTimeField, this.factor.negate(), this.intercept.negate());
        }

        // 常量因子相加
        public void addFactor(ConstraintValue factor) {
            this.factor = new ConstantValue(this.factor.getBigDecimalConstant()
                    .add(factor.getBigDecimalConstant()).toPlainString());
        }

        // 常量因子相减
        public void subFactor(ConstraintValue factor) {
            this.factor = new ConstantValue(this.factor.getBigDecimalConstant()
                    .subtract(factor.getBigDecimalConstant()).toPlainString());
        }

        // 常量因子相乘
        public void multiplyFactor(ConstraintValue factor) {
            this.factor = new ConstantValue(this.factor.getBigDecimalConstant()
                    .multiply(factor.getBigDecimalConstant()).toPlainString());
            this.intercept = new ConstantValue(this.intercept.getBigDecimalConstant()
                    .multiply(factor.getBigDecimalConstant()).toPlainString());
        }

        // 常量截距相加
        public void addIntercept(ConstraintValue intercept) {
            this.intercept = new ConstantValue(this.intercept.getBigDecimalConstant()
                    .add(intercept.getBigDecimalConstant()).toPlainString());
        }

        // 常量截距相减
        public void subIntercept(ConstraintValue intercept) {
            this.intercept = new ConstantValue(this.intercept.getBigDecimalConstant()
                    .subtract(intercept.getBigDecimalConstant()).toPlainString());
        }

        // 取消常量截距
        public void setInterceptZero() {
            this.intercept = createConstantConstraintValue("0");
        }

        // 转换为倒数
        public void transToReciprocalVal(ConstraintValue constant) {
            // TODO 目前仅支持对ts时序对象相除,不支持对复合算术运算表达式相除：1 / (k * ts) + c等嵌套表达式
            if (this.factor.getBigDecimalConstant().compareTo(BigDecimal.ONE) == 0
                    || this.intercept.getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0)
                throw new IgnoreMeException();
            this.isReciprocalVal = true;
            this.factor = constant;
        }

        public TimeSeriesValue transToReciprocalVal() {
            this.isReciprocalVal = true;
            return this;
        }

        public BigDecimal eliminateFactor(BigDecimal constant) {
            // 仅消除常量因子, 变量因子在最后验证结果集运算时才消除
            // 示例: factor * time series + intercept >= constant
            // 执行消除常量因子, factor在修改因子各位置已进行控制, 必不为0
            assert !this.factor.valueEqualZero();
            BigDecimal result = constant.subtract(this.intercept.getBigDecimalConstant())
                    .divide(this.factor.getBigDecimalConstant(), DoubleArithmeticPrecisionConstant.scale,
                            RoundingMode.HALF_UP);
            // 因子归于0
            this.factor = new ConstantValue(BigDecimal.ONE.toPlainString());
            this.intercept = new ConstantValue(BigDecimal.ZERO.toPlainString());
            return result;
        }

        public ConstraintValue getFactor() {
            return factor;
        }

        public ConstraintValue getIntercept() {
            return intercept;
        }

        public boolean hasNegativeFactor() {
            return this.factor.getBigDecimalConstant().compareTo(new BigDecimal(0)) < 0;
        }

        @Override
        public TimeSeriesValue transToBaseSeq(String databaseName, String tableName) {
            if (this.timeSeriesName.equalsIgnoreCase(GlobalConstant.BASE_TIME_SERIES_NAME)
                    || this.timeSeriesName.equalsIgnoreCase(GlobalConstant.TIME_FIELD_NAME))
                return this;
            // 转换为基础列约束
            return EquationsManager.getInstance()
                    .getEquationsFromTimeSeries(databaseName, tableName, this.timeSeriesName)
                    .transformBaseTimeSeriesValue(factor.getBigDecimalConstant(), intercept.getBigDecimalConstant());
        }

        public static class TimeSeriesValueParams {
            private ConstraintValue factor;
            private ConstraintValue intercept;

            public TimeSeriesValueParams(BigDecimal factor, BigDecimal intercept) {
                this.factor = new ConstantValue(factor.toPlainString());
                this.intercept = new ConstantValue(intercept.toPlainString());
            }

            public ConstraintValue getFactor() {
                return factor;
            }

            public ConstraintValue getIntercept() {
                return intercept;
            }
        }
    }

    public static class ConstantValue extends ConstraintValue {
        private final String value;

        public ConstantValue(String value) {
            this.value = value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String getConstant() {
            return value;
        }

        @Override
        public BigDecimal getBigDecimalConstant() {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                log.warn("ConstantValue getBigDecimalConstant 异常: 字符串不支持");
                throw new IgnoreMeException();
            }
        }

        public boolean valueEqualZero() {
            return getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0;
        }

        @Override
        public ConstraintValue negate() {
            try {
                BigDecimal bigDecimal = new BigDecimal(value);
                return new ConstantValue(bigDecimal.negate().toPlainString());
            } catch (NumberFormatException e) {
                // 字符串不支持转为负数
                log.warn("ConstantValue negate 异常: 字符串不支持转为负数");
                throw new IgnoreMeException();
            }
        }
    }

    public static class OperatorValue extends ConstraintValue {
        private final String op;

        public OperatorValue(String op) {
            this.op = op;
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        public String getOperator() {
            return op;
        }
    }

    public static class TimeSeriesConstraintValue extends ConstraintValue {
        // TODO 增加Y轴过滤条件 -> Y(TimeSeriesConstraint), 1/Y(TimeSeriesConstraint)
        // 针对基础列Y
        // 分为正数和负数两种情况
        private final TimeSeriesValue timeSeriesValue;
        private final TimeSeriesConstraint timeSeriesConstraint;

        public TimeSeriesConstraintValue(TimeSeriesValue timeSeriesValue, TimeSeriesConstraint timeSeriesConstraint) {
            this.timeSeriesConstraint = timeSeriesConstraint;
            this.timeSeriesValue = timeSeriesValue;
            if (!timeSeriesValue.getTimeSeriesName().equalsIgnoreCase(timeSeriesConstraint.getTimeSeriesName()))
                log.error("命名有误: {} {}", timeSeriesValue, timeSeriesConstraint);
        }

        @Override
        public boolean isTimeSeriesConstraint() {
            return true;
        }

        @Override
        public TimeSeriesConstraint getTimeSeriesConstraint() {
            return timeSeriesConstraint;
        }

        @Override
        public TimeSeriesConstraintValue transToBaseSeq(String databaseName, String tableName) {
            if (!timeSeriesValue.isReciprocalVal
                    && timeSeriesConstraint.getTimeSeriesName().equalsIgnoreCase(GlobalConstant.BASE_TIME_SERIES_NAME))
                return this;
            else if (timeSeriesConstraint.getTimeSeriesName().equalsIgnoreCase(GlobalConstant.TIME_FIELD_NAME)) {
                return SamplingFrequencyManager.getInstance().getSamplingFrequencyFromCollection(databaseName, tableName)
                        .transformToBaseSeqConstraintValue(this);
            }

            // 倒数转换
            Equations equationsFromTimeSeries = EquationsManager.getInstance()
                    .getEquationsFromTimeSeries(databaseName, tableName, timeSeriesConstraint.getTimeSeriesName());
            if (timeSeriesValue.isReciprocalVal) {
                equationsFromTimeSeries = EquationsManager.generateReciprocalEquations(
                        timeSeriesValue.getFactor().getBigDecimalConstant(),
                        timeSeriesValue.getIntercept().getBigDecimalConstant());
            }

            // 转换为基础列约束
            return new TimeSeriesConstraintValue(ConstraintValueGenerator.genBaseTimeSeries(),
                    timeSeriesConstraint.transConstraintByEquations(equationsFromTimeSeries));
        }

        @Override
        public ConstraintValue negate() {
            // 取反集
            return new TimeSeriesConstraintValue(this.timeSeriesValue, this.timeSeriesConstraint.complement());
        }

        @Override
        public TimeSeriesValue getTimeSeriesValue() {
            return timeSeriesValue;
        }
    }

    public String getTimeSeriesName() {
        throw new UnsupportedOperationException();
    }

    public String getConstant() {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimalConstant() {
        throw new UnsupportedOperationException();
    }

    public ConstraintValue transToBaseSeq(String databaseName, String tableName) {
        throw new UnsupportedOperationException();
    }

    public String getOperator() {
        throw new UnsupportedOperationException();
    }

    public TimeSeriesValue getTimeSeriesValue() {
        throw new UnsupportedOperationException();
    }

    public static Long counter = 0L;

    public TimeSeriesConstraint getTimeSeriesConstraint() {
        counter++;
        log.warn("非列约束调用getTimeSeriesConstraint 异常");
        throw new AssertionError();
    }

    public ConstraintValue negate() {
        throw new UnsupportedOperationException();
    }

    private static ConstantValue createConstantConstraintValue(String value) {
        return new ConstantValue(value);
    }
}
