package com.fuzzy.influxdb;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.influxdb.ast.*;

public class InfluxDBExpectedValueVisitor implements InfluxDBVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(InfluxDBExpression expr) {
        InfluxDBToStringVisitor v = new InfluxDBToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(InfluxDBTableReference ref) {

    }

    @Override
    public void visit(InfluxDBConstant constant) {
        print(constant);
    }

    @Override
    public void visit(InfluxDBColumnReference column) {
        print(column);
    }

    @Override
    public void visit(InfluxDBBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(InfluxDBSelect select) {
        // TODO
        for (InfluxDBExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(InfluxDBBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(InfluxDBCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
     public void visit(InfluxDBBinaryBitwiseOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(InfluxDBBinaryArithmeticOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(InfluxDBOrderByTerm op) {

    }

    @Override
    public void visit(InfluxDBStringExpression op) {
        print(op);
    }

    @Override
    public void visit(InfluxDBUnaryNotPrefixOperation op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == UnaryOperation.OperatorKind.PREFIX) {
            sb.append(op.getOperatorRepresentation());
            sb.append(' ');
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        visit(op.getExpression());
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
        if (op.getOperatorKind() == UnaryOperation.OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

    @Override
    public void visit(InfluxDBUnaryPrefixOperation op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == UnaryOperation.OperatorKind.PREFIX) {
            sb.append(op.getOperatorRepresentation());
            sb.append(' ');
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        visit(op.getExpression());
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
        if (op.getOperatorKind() == UnaryOperation.OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

    @Override
    public void visit(InfluxDBExpression expr) {
        nrTabs++;
        try {
            InfluxDBVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    public String get() {
        return sb.toString();
    }
}
