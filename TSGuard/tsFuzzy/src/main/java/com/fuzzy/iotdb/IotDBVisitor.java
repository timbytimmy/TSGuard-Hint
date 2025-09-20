package com.fuzzy.iotdb;


import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.iotdb.ast.*;

import java.util.Set;

public interface IotDBVisitor {

    void visit(IotDBTableReference ref);

    void visit(IotDBSchemaReference ref);

    void visit(IotDBConstant constant);

    void visit(IotDBColumnReference column);

    void visit(IotDBBinaryLogicalOperation op);

    void visit(IotDBSelect select);

    void visit(IotDBBinaryComparisonOperation op);

    void visit(IotDBCastOperation op);

    void visit(IotDBBinaryArithmeticOperation op);

    void visit(IotDBOrderByTerm op);

    void visit(IotDBUnaryPostfixOperation op);

    void visit(IotDBInOperation op);

    void visit(IotDBBetweenOperation op);

    void visit(IotDBUnaryNotPrefixOperation op);

    void visit(IotDBUnaryPrefixOperation op);

//    void visit(IotDBExists op);

//    void visit(IotDBStringExpression op);

//    void visit(IotDBComputableFunction f);

//    void visit(IotDBCollate collate);

//    void visit(IotDBBinaryOperation op);

    default void visit(IotDBExpression expr) {
        if (expr instanceof IotDBConstant) {
            visit((IotDBConstant) expr);
        } else if (expr instanceof IotDBColumnReference) {
            visit((IotDBColumnReference) expr);
        } else if (expr instanceof IotDBBinaryLogicalOperation) {
            visit((IotDBBinaryLogicalOperation) expr);
        } else if (expr instanceof IotDBCastOperation) {
            visit((IotDBCastOperation) expr);
        } else if (expr instanceof IotDBSelect) {
            visit((IotDBSelect) expr);
        } else if (expr instanceof IotDBBinaryComparisonOperation) {
            visit((IotDBBinaryComparisonOperation) expr);
        } else if (expr instanceof IotDBOrderByTerm) {
            visit((IotDBOrderByTerm) expr);
        } else if (expr instanceof IotDBUnaryPostfixOperation) {
            visit((IotDBUnaryPostfixOperation) expr);
        } else if (expr instanceof IotDBBinaryArithmeticOperation) {
            visit((IotDBBinaryArithmeticOperation) expr);
        }/*else if (expr instanceof IotDBBinaryOperation) {
            visit((IotDBBinaryOperation) expr);
        } else if (expr instanceof IotDBExists) {
            visit((IotDBExists) expr);
        } else if (expr instanceof IotDBStringExpression) {
            visit((IotDBStringExpression) expr);
        } */ else if (expr instanceof IotDBTableReference) {
            visit((IotDBTableReference) expr);
        } else if (expr instanceof IotDBSchemaReference) {
            visit((IotDBSchemaReference) expr);
        } else if (expr instanceof IotDBInOperation) {
            visit((IotDBInOperation) expr);
        } else if (expr instanceof IotDBBetweenOperation) {
            visit((IotDBBetweenOperation) expr);
        } /*else if (expr instanceof IotDBComputableFunction) {
            visit((IotDBComputableFunction) expr);
        } else if (expr instanceof IotDBCollate) {
            visit((IotDBCollate) expr);
        } */ else if (expr instanceof IotDBUnaryNotPrefixOperation) {
            visit((IotDBUnaryNotPrefixOperation) expr);
        } else if (expr instanceof IotDBUnaryPrefixOperation) {
            visit((IotDBUnaryPrefixOperation) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(IotDBExpression expr) {
        IotDBToStringVisitor visitor = new IotDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asString(IotDBExpression expr, boolean isAbstractExpression) {
        IotDBToStringVisitor visitor = new IotDBToStringVisitor(isAbstractExpression);
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(IotDBExpression expr) {
        IotDBExpectedValueVisitor visitor = new IotDBExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static TimeSeriesConstraint asConstraint(String databaseName, String tableName,
                                             IotDBExpression expr, Set<Long> nullValuesSet) {
        IotDBToConstraintVisitor visitor = new IotDBToConstraintVisitor(databaseName, tableName, nullValuesSet);
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
