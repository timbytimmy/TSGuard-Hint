package com.fuzzy.TDengine;

import com.fuzzy.TDengine.ast.*;
import com.fuzzy.TDengine.ast.TDengineOrderByTerm.TDengineOrder;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.jdcloud.sdk.utils.StringUtils;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class TDengineToStringVisitor extends ToStringVisitor<TDengineExpression> implements TDengineVisitor {

    int ref;
    // 是否抽象语法节点
    boolean isAbstractExpression;

    public TDengineToStringVisitor() {
        isAbstractExpression = false;
    }

    public TDengineToStringVisitor(boolean isAbstractExpression) {
        this.isAbstractExpression = isAbstractExpression;
    }

    @Override
    public void visitSpecific(TDengineExpression expr) {
        TDengineVisitor.super.visit(expr);
    }

    @Override
    public void visit(TDengineTableReference ref) {
        if (!isAbstractExpression) sb.append(ref.getTable().getName());
        else sb.append(GlobalConstant.TABLE_NAME);
    }

    @Override
    public void visit(TDengineSchemaReference ref) {
        if (!isAbstractExpression) sb.append(ref.getSchema().getRandomTable().getDatabaseName());
        else sb.append(GlobalConstant.DATABASE_NAME);
    }

    @Override
    public void visit(TDengineConstant constant) {
        if (!isAbstractExpression) sb.append(constant.getTextRepresentation());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(TDengineColumnReference column) {
        String columnName = column.getColumn().getName();
        if (isAbstractExpression && !columnName.equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName()))
            columnName = GlobalConstant.COLUMN_NAME;
        sb.append(columnName);
    }

    public void visitDoubleValueLeft(TDengineConstant constant) {
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .subtract(BigDecimal.valueOf(Math.pow(10, -TDengineConstant.TDengineDoubleConstant.scale)))
                    .setScale(TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
        sb.append(" <= ");
    }

    public void visitDoubleValueRight(TDengineConstant constant) {
        sb.append(" <= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .add(BigDecimal.valueOf(Math.pow(10, -TDengineConstant.TDengineDoubleConstant.scale)))
                    .setScale(TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(TDengineBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");

        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");

        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(TDengineSelect s) {
        sb.append("SELECT ");
        if (s.getHint() != null) {
            sb.append("/*+ ");
            visit(s.getHint());
            sb.append("*/ ");
        }
        switch (s.getFromOptions()) {
            case DISTINCT:
                sb.append("DISTINCT ");
                break;
            case ALL:
//                sb.append(Randomly.fromOptions("ALL ", ""));
                break;
            default:
                throw new AssertionError();
        }

        // 常规查询
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) sb.append(", ");
                // not support cross join
                visit(s.getFetchColumns().get(i));
                if (i != s.getFetchColumns().size() - 1) sb.append(" AS ").append(TDengineConstantString.REF).append(i);
            }
        }

        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
        if (s.getWhereClause() != null) {
            TDengineExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && s.getGroupByExpressions().size() > 0) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<TDengineExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }

        if (!ObjectUtils.isEmpty(s.getIntervalValues())) {
            sb.append(" INTERVAL (");
            for (int i = 0; i < s.getIntervalValues().size(); i++) {
                sb.append(s.getIntervalValues().get(i));
                if (i != s.getIntervalValues().size() - 1) sb.append(", ");
            }
            sb.append(")");
        }

        if (!StringUtils.isBlank(s.getSlidingValue())) {
            sb.append(" SLIDING (")
                    .append(s.getSlidingValue())
                    .append(")");
        }


        if (!ObjectUtils.isEmpty(s.getOrderByExpressions())) {
            sb.append(" ORDER BY ");
            List<TDengineExpression> orderBys = s.getOrderByExpressions();
            for (int i = 0; i < orderBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getOrderByExpressions().get(i));
            }
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(TDengineBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(TDengineCastOperation op) {
        sb.append(" CAST(");
        visit(op.getExpr());
        sb.append(" as ");
        sb.append(op.getCastType());
        sb.append(")");
    }

    @Override
    public void visit(TDengineBinaryOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(TDengineUnaryPrefixOperation op) {
        super.visit((UnaryOperation<TDengineExpression>) op);
    }

    @Override
    public void visit(TDengineUnaryNotPrefixOperation unaryOperation) {
        // Unary NOT Prefix Operation
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        TDengineUnaryNotPrefixOperation.TDengineUnaryNotPrefixOperator op =
                ((TDengineUnaryNotPrefixOperation) unaryOperation).getOp();
        // NOT DOUBLE
        if (op.equals(TDengineUnaryNotPrefixOperation.TDengineUnaryNotPrefixOperator.NOT_DOUBLE)) {
            visitDoubleValueLeft(unaryOperation.getExpression().getExpectedValue());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
            visit(unaryOperation.getExpression());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            sb.append(" AND ");
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
            visit(unaryOperation.getExpression());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            visitDoubleValueRight(unaryOperation.getExpression().getExpectedValue());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            return;
        }

        // NOT BOOLEAN
        if (op.equals(TDengineUnaryNotPrefixOperation.TDengineUnaryNotPrefixOperator.NOT))
            sb.append(unaryOperation.getOperatorRepresentation());
        else {
            // NOT INT
            visit(unaryOperation.getExpression().getExpectedValue());
            // 子节点为常量、列、强转运算时, NOT符号改为 =
            if (unaryOperation.getExpression() instanceof TDengineConstant
                    || unaryOperation.getExpression() instanceof TDengineColumnReference
                    || unaryOperation.getExpression() instanceof TDengineCastOperation
                    || unaryOperation.getExpression() instanceof TDengineUnaryPrefixOperation
                    || unaryOperation.getExpression() instanceof TDengineBinaryArithmeticOperation
                    || unaryOperation.getExpression() instanceof TDengineComputableFunction
                    || unaryOperation.getExpression() instanceof TDengineBinaryOperation)
                sb.append(" = ");
        }

        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        visit(unaryOperation.getExpression());
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
    }

    @Override
    public void visit(TDengineBinaryArithmeticOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(TDengineOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == TDengineOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(TDengineUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }
        switch (op.getOperator()) {
            case IS_NULL:
                sb.append("NULL");
                break;
            default:
                throw new AssertionError(op);
        }
    }

//    @Override
//    public void visit(TDengineExists op) {
//        // TODO
//        sb.append(" EXISTS (");
//        visit(op.getExpr());
//        sb.append(")");
//    }

//    @Override
//    public void visit(TDengineStringExpression op) {
//        sb.append(op.getStr());
//    }

    @Override
    public void visit(TDengineInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) sb.append(" NOT");
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) sb.append(", ");
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(TDengineBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(UnaryOperation<TDengineExpression> unaryOperation) {
        if (unaryOperation instanceof TDengineUnaryNotPrefixOperation)
            visit((TDengineUnaryNotPrefixOperation) unaryOperation);
        else if (unaryOperation instanceof TDengineUnaryPrefixOperation)
            visit((TDengineUnaryPrefixOperation) unaryOperation);
    }

    @Override
    public void visit(TDengineComputableFunction f) {
        sb.append(f.getFunction().getName());
        sb.append("(");
        for (int i = 0; i < f.getArguments().length; i++) {
            if (i != 0) sb.append(", ");
            visit(f.getArguments()[i]);
        }
        sb.append(")");
    }

}
