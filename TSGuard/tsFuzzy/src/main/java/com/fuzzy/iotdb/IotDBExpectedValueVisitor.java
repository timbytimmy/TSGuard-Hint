package com.fuzzy.iotdb;


import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.iotdb.ast.*;

public class IotDBExpectedValueVisitor implements IotDBVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(IotDBExpression expr) {
        IotDBToStringVisitor v = new IotDBToStringVisitor();
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
    public void visit(IotDBTableReference ref) {
        print(ref);
    }

    @Override
    public void visit(IotDBSchemaReference ref) {
        print(ref);
    }

    @Override
    public void visit(IotDBConstant constant) {
        print(constant);
    }

    @Override
    public void visit(IotDBColumnReference column) {
        print(column);
    }

    @Override
    public void visit(IotDBBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(IotDBSelect select) {
        for (IotDBExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(IotDBBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(IotDBCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(IotDBBinaryArithmeticOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    //    @Override
//    public void visit(IotDBBinaryOperation op) {
//        print(op);
//        visit(op.getLeft());
//        visit(op.getRight());
//    }
//
    @Override
    public void visit(IotDBOrderByTerm op) {

    }

    @Override
    public void visit(IotDBUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(IotDBInOperation op) {
        print(op);
        for (IotDBExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(IotDBBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(IotDBUnaryNotPrefixOperation op) {
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
    public void visit(IotDBUnaryPrefixOperation op) {
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

//
//    @Override
//    public void visit(IotDBExists op) {
//        print(op);
//        visit(op.getExpr());
//    }
//
//    @Override
//    public void visit(IotDBStringExpression op) {
//        print(op);
//    }

    @Override
    public void visit(IotDBExpression expr) {
        nrTabs++;
        try {
            IotDBVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    public String get() {
        return sb.toString();
    }
}
