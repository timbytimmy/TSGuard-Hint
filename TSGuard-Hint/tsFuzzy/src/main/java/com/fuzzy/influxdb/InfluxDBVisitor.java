package com.fuzzy.influxdb;

import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.influxdb.ast.*;

import java.util.Set;

public interface InfluxDBVisitor {

    void visit(InfluxDBTableReference ref);

    void visit(InfluxDBConstant constant);

    void visit(InfluxDBColumnReference column);

    void visit(InfluxDBBinaryLogicalOperation op);

    void visit(InfluxDBSelect select);

    void visit(InfluxDBBinaryComparisonOperation op);

    void visit(InfluxDBCastOperation op);

    void visit(InfluxDBBinaryBitwiseOperation op);

    void visit(InfluxDBBinaryArithmeticOperation op);

    void visit(InfluxDBOrderByTerm op);

    void visit(InfluxDBStringExpression op);

    void visit(InfluxDBUnaryNotPrefixOperation op);

    void visit(InfluxDBUnaryPrefixOperation op);

    // void visit(InfluxDBComputableFunction f);
    // void visit(InfluxDBInOperation op);
    // void visit(InfluxDBBetweenOperation op);
    // void visit(InfluxDBCollate collate);

    default void visit(InfluxDBExpression expr) {
        if (expr instanceof InfluxDBConstant) {
            visit((InfluxDBConstant) expr);
        } else if (expr instanceof InfluxDBColumnReference) {
            visit((InfluxDBColumnReference) expr);
        } else if (expr instanceof InfluxDBBinaryLogicalOperation) {
            visit((InfluxDBBinaryLogicalOperation) expr);
        } else if (expr instanceof InfluxDBSelect) {
            visit((InfluxDBSelect) expr);
        } else if (expr instanceof InfluxDBBinaryComparisonOperation) {
            visit((InfluxDBBinaryComparisonOperation) expr);
        } else if (expr instanceof InfluxDBCastOperation) {
            visit((InfluxDBCastOperation) expr);
        } else if (expr instanceof InfluxDBBinaryBitwiseOperation) {
            visit((InfluxDBBinaryBitwiseOperation) expr);
        } else if (expr instanceof InfluxDBBinaryArithmeticOperation) {
            visit((InfluxDBBinaryArithmeticOperation) expr);
        } else if (expr instanceof InfluxDBOrderByTerm) {
            visit((InfluxDBOrderByTerm) expr);
        } else if (expr instanceof InfluxDBStringExpression) {
            visit((InfluxDBStringExpression) expr);
        } else if (expr instanceof InfluxDBTableReference) {
            visit((InfluxDBTableReference) expr);
        } /*else if (expr instanceof InfluxDBComputableFunction) {
            visit((InfluxDBComputableFunction) expr);
        } else if (expr instanceof InfluxDBCollate) {
            visit((InfluxDBCollate) expr);
        } */ else if (expr instanceof InfluxDBUnaryNotPrefixOperation) {
            visit((InfluxDBUnaryNotPrefixOperation) expr);
        } else if (expr instanceof InfluxDBUnaryPrefixOperation) {
            visit((InfluxDBUnaryPrefixOperation) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(InfluxDBExpression expr) {
        InfluxDBToStringVisitor visitor = new InfluxDBToStringVisitor();
        visitor.visit(expr);
//        return visitor.get();
        // TODO reported bug: (time)
        return visitor.get().replace("(time)", "time");
    }

    static String asString(InfluxDBExpression expr, boolean isAbstractExpression) {
        InfluxDBToStringVisitor visitor = new InfluxDBToStringVisitor(isAbstractExpression);
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(InfluxDBExpression expr) {
        InfluxDBExpectedValueVisitor visitor = new InfluxDBExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static TimeSeriesConstraint asConstraint(String databaseName, String tableName,
                                             InfluxDBExpression expr, Set<Long> nullValuesSet) {
        InfluxDBToConstraintVisitor visitor = new InfluxDBToConstraintVisitor(databaseName, tableName, nullValuesSet);
        visitor.visit(expr);
        TimeSeriesConstraint timeSeriesConstraint = visitor.constraintStack.pop().getTimeSeriesConstraint();
//        if (!expr.hasColumn()) return columnConstraint;
        // 存在列运算, 进行空值过滤(时间戳)
        TimeSeriesConstraint nullValueFilter = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME,
                new RangeConstraint());
        nullValuesSet.forEach(timestamp -> nullValueFilter.addNotEqualValue(SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, tableName).getSeqByTimestamp(timestamp)));
        return timeSeriesConstraint.intersects(nullValueFilter);
    }

}
