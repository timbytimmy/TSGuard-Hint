package com.fuzzy.griddb;


import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.griddb.ast.*;

import java.util.Set;

public interface GridDBVisitor {

    void visit(GridDBTableReference ref);

    void visit(GridDBSchemaReference ref);

    void visit(GridDBConstant constant);

    void visit(GridDBColumnReference column);

    void visit(GridDBBinaryLogicalOperation op);

    void visit(GridDBSelect select);

    void visit(GridDBBinaryComparisonOperation op);

    void visit(GridDBCastOperation op);

    void visit(GridDBBinaryOperation op);

    void visit(GridDBUnaryPrefixOperation op);

    void visit(GridDBUnaryNotPrefixOperation op);

    void visit(GridDBBinaryArithmeticOperation op);

    void visit(GridDBOrderByTerm op);

    void visit(GridDBUnaryPostfixOperation op);

    void visit(GridDBInOperation op);

    void visit(GridDBBetweenOperation op);

//    void visit(GridDBExists op);
//
//    void visit(GridDBStringExpression op);

    void visit(GridDBComputableFunction f);

    // void visit(GridDBCollate collate);

    default void visit(GridDBExpression expr) {
        if (expr instanceof GridDBConstant) {
            visit((GridDBConstant) expr);
        } else if (expr instanceof GridDBColumnReference) {
            visit((GridDBColumnReference) expr);
        } else if (expr instanceof GridDBBinaryLogicalOperation) {
            visit((GridDBBinaryLogicalOperation) expr);
        } else if (expr instanceof GridDBCastOperation) {
            visit((GridDBCastOperation) expr);
        } else if (expr instanceof GridDBSelect) {
            visit((GridDBSelect) expr);
        } else if (expr instanceof GridDBBinaryComparisonOperation) {
            visit((GridDBBinaryComparisonOperation) expr);
        } else if (expr instanceof GridDBOrderByTerm) {
            visit((GridDBOrderByTerm) expr);
        } else if (expr instanceof GridDBUnaryPostfixOperation) {
            visit((GridDBUnaryPostfixOperation) expr);
        } else if (expr instanceof GridDBBinaryArithmeticOperation) {
            visit((GridDBBinaryArithmeticOperation) expr);
        } else if (expr instanceof GridDBBinaryOperation) {
            visit((GridDBBinaryOperation) expr);
        } /*else if (expr instanceof GridDBExists) {
            visit((GridDBExists) expr);
        }*/ else if (expr instanceof GridDBTableReference) {
            visit((GridDBTableReference) expr);
        } else if (expr instanceof GridDBSchemaReference) {
            visit((GridDBSchemaReference) expr);
        } else if (expr instanceof GridDBInOperation) {
            visit((GridDBInOperation) expr);
        } else if (expr instanceof GridDBBetweenOperation) {
            visit((GridDBBetweenOperation) expr);
        } else if (expr instanceof GridDBUnaryPrefixOperation) {
            visit((GridDBUnaryPrefixOperation) expr);
        } else if (expr instanceof GridDBUnaryNotPrefixOperation) {
            visit((GridDBUnaryNotPrefixOperation) expr);
        } else if (expr instanceof GridDBComputableFunction) {
            visit((GridDBComputableFunction) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(GridDBExpression expr) {
        GridDBToStringVisitor visitor = new GridDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asString(GridDBExpression expr, boolean isAbstractExpression) {
        GridDBToStringVisitor visitor = new GridDBToStringVisitor(isAbstractExpression);
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(GridDBExpression expr) {
        GridDBExpectedValueVisitor visitor = new GridDBExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static TimeSeriesConstraint asConstraint(String databaseName, String tableName,
                                             GridDBExpression expr, Set<Long> nullValuesSet) {
        GridDBToConstraintVisitor visitor = new GridDBToConstraintVisitor(databaseName, tableName, nullValuesSet);
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
