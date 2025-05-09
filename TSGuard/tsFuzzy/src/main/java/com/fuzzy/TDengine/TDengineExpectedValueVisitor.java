package com.fuzzy.TDengine;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.TDengine.ast.*;
import com.fuzzy.common.visitor.UnaryOperation.OperatorKind;

public class TDengineExpectedValueVisitor implements TDengineVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(TDengineExpression expr) {
        TDengineToStringVisitor v = new TDengineToStringVisitor();
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
    public void visit(TDengineTableReference ref) {
        print(ref);
    }

    @Override
    public void visit(TDengineSchemaReference ref) {
        print(ref);
    }

    @Override
    public void visit(TDengineConstant constant) {
        print(constant);
    }

    @Override
    public void visit(TDengineColumnReference column) {
        print(column);
    }

    @Override
    public void visit(TDengineBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(TDengineSelect select) {
        for (TDengineExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(TDengineBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(TDengineCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(TDengineBinaryArithmeticOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(TDengineBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(TDengineUnaryPrefixOperation op) {
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
    public void visit(TDengineUnaryNotPrefixOperation op) {
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
    public void visit(TDengineOrderByTerm op) {

    }

    @Override
    public void visit(TDengineUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(TDengineInOperation op) {
        print(op);
        for (TDengineExpression right : op.getListElements())
            visit(right);
    }

    @Override
    public void visit(TDengineBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

//    @Override
//    public void visit(TDengineExists op) {
//        print(op);
//        visit(op.getExpr());
//    }
//
//    @Override
//    public void visit(TDengineStringExpression op) {
//        print(op);
//    }

    @Override
    public void visit(TDengineComputableFunction f) {
        print(f);
        for (TDengineExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(TDengineExpression expr) {
        nrTabs++;
        try {
            TDengineVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    public String get() {
        return sb.toString();
    }
}
