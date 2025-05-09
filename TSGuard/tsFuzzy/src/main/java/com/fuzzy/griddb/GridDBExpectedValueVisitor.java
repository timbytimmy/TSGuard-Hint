package com.fuzzy.griddb;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.visitor.UnaryOperation.OperatorKind;
import com.fuzzy.griddb.ast.*;

public class GridDBExpectedValueVisitor implements GridDBVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(GridDBExpression expr) {
        GridDBToStringVisitor v = new GridDBToStringVisitor();
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
    public void visit(GridDBTableReference ref) {
        print(ref);
    }

    @Override
    public void visit(GridDBSchemaReference ref) {
        print(ref);
    }

    @Override
    public void visit(GridDBConstant constant) {
        print(constant);
    }

    @Override
    public void visit(GridDBColumnReference column) {
        print(column);
    }

    @Override
    public void visit(GridDBBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GridDBSelect select) {
        for (GridDBExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(GridDBBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GridDBCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(GridDBBinaryArithmeticOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GridDBBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GridDBUnaryPrefixOperation op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == OperatorKind.PREFIX) {
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
        if (op.getOperatorKind() == OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

    @Override
    public void visit(GridDBUnaryNotPrefixOperation op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == OperatorKind.PREFIX) {
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
        if (op.getOperatorKind() == OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

    @Override
    public void visit(GridDBOrderByTerm op) {

    }

    @Override
    public void visit(GridDBUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(GridDBInOperation op) {
        print(op);
        for (GridDBExpression right : op.getListElements())
            visit(right);
    }

    @Override
    public void visit(GridDBBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(GridDBComputableFunction f) {
        print(f);
        for (GridDBExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

//    @Override
//    public void visit(GridDBExists op) {
//        print(op);
//        visit(op.getExpr());
//    }
//
//    @Override
//    public void visit(GridDBStringExpression op) {
//        print(op);
//    }

    @Override
    public void visit(GridDBExpression expr) {
        nrTabs++;
        try {
            GridDBVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    public String get() {
        return sb.toString();
    }
}
