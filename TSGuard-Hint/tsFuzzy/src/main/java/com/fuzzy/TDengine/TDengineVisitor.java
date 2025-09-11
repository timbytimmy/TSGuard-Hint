package com.fuzzy.TDengine;


import com.fuzzy.TDengine.ast.*;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;

import java.util.Set;

public interface TDengineVisitor {

    void visit(TDengineTableReference ref);

    void visit(TDengineSchemaReference ref);

    void visit(TDengineConstant constant);

    void visit(TDengineColumnReference column);

    void visit(TDengineBinaryLogicalOperation op);

    void visit(TDengineSelect select);

    void visit(TDengineBinaryComparisonOperation op);

    void visit(TDengineCastOperation op);

    void visit(TDengineBinaryOperation op);

    void visit(TDengineUnaryPrefixOperation op);

    void visit(TDengineUnaryNotPrefixOperation op);

    void visit(TDengineBinaryArithmeticOperation op);

    void visit(TDengineOrderByTerm op);

    void visit(TDengineUnaryPostfixOperation op);

    void visit(TDengineInOperation op);

    void visit(TDengineBetweenOperation op);

//    void visit(TDengineExists op);
//
//    void visit(TDengineStringExpression op);

    void visit(TDengineComputableFunction f);

    // void visit(TDengineCollate collate);

    default void visit(TDengineExpression expr) {
        if (expr instanceof TDengineConstant) {
            visit((TDengineConstant) expr);
        } else if (expr instanceof TDengineColumnReference) {
            visit((TDengineColumnReference) expr);
        } else if (expr instanceof TDengineBinaryLogicalOperation) {
            visit((TDengineBinaryLogicalOperation) expr);
        } else if (expr instanceof TDengineCastOperation) {
            visit((TDengineCastOperation) expr);
        } else if (expr instanceof TDengineSelect) {
            visit((TDengineSelect) expr);
        } else if (expr instanceof TDengineBinaryComparisonOperation) {
            visit((TDengineBinaryComparisonOperation) expr);
        } else if (expr instanceof TDengineOrderByTerm) {
            visit((TDengineOrderByTerm) expr);
        } else if (expr instanceof TDengineUnaryPostfixOperation) {
            visit((TDengineUnaryPostfixOperation) expr);
        } else if (expr instanceof TDengineBinaryArithmeticOperation) {
            visit((TDengineBinaryArithmeticOperation) expr);
        } else if (expr instanceof TDengineBinaryOperation) {
            visit((TDengineBinaryOperation) expr);
        } /*else if (expr instanceof TDengineExists) {
            visit((TDengineExists) expr);
        } */ else if (expr instanceof TDengineTableReference) {
            visit((TDengineTableReference) expr);
        } else if (expr instanceof TDengineSchemaReference) {
            visit((TDengineSchemaReference) expr);
        } else if (expr instanceof TDengineInOperation) {
            visit((TDengineInOperation) expr);
        } else if (expr instanceof TDengineBetweenOperation) {
            visit((TDengineBetweenOperation) expr);
        } else if (expr instanceof TDengineUnaryPrefixOperation) {
            visit((TDengineUnaryPrefixOperation) expr);
        } else if (expr instanceof TDengineUnaryNotPrefixOperation) {
            visit((TDengineUnaryNotPrefixOperation) expr);
        } else if (expr instanceof TDengineComputableFunction) {
            visit((TDengineComputableFunction) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(TDengineExpression expr) {
        TDengineToStringVisitor visitor = new TDengineToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asString(TDengineExpression expr, boolean isAbstractExpression) {
        TDengineToStringVisitor visitor = new TDengineToStringVisitor(isAbstractExpression);
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(TDengineExpression expr) {
        TDengineExpectedValueVisitor visitor = new TDengineExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static TimeSeriesConstraint asConstraint(String databaseName, String tableName,
                                             TDengineExpression expr, Set<Long> nullValuesSet) {
        TDengineToConstraintVisitor visitor = new TDengineToConstraintVisitor(databaseName, tableName, nullValuesSet);
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
