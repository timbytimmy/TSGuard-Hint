package com.fuzzy.iotdb;

import com.fuzzy.Randomly;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.ast.IotDBOrderByTerm.IotDBOrder;
import com.fuzzy.iotdb.util.IotDBValueStateConstant;
import com.jdcloud.sdk.utils.StringUtils;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class IotDBToStringVisitor extends ToStringVisitor<IotDBExpression> implements IotDBVisitor {

    int ref;
    // 是否抽象语法节点
    boolean isAbstractExpression;

    public IotDBToStringVisitor() {
        this.isAbstractExpression = false;
    }

    public IotDBToStringVisitor(boolean isAbstractExpression) {
        this.isAbstractExpression = isAbstractExpression;
    }

    @Override
    public void visitSpecific(IotDBExpression expr) {
        IotDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(IotDBTableReference ref) {
        if (!isAbstractExpression) sb.append(ref.getTable().getDatabaseName())
                .append(".").append(ref.getTable().getName());
        else sb.append(GlobalConstant.TABLE_NAME);
    }

    @Override
    public void visit(IotDBSchemaReference ref) {
        if (!isAbstractExpression) sb.append(ref.getSchema().getRandomTable().getDatabaseName());
        else sb.append(GlobalConstant.DATABASE_NAME);
    }

    @Override
    public void visit(IotDBConstant constant) {
        if (!isAbstractExpression) sb.append(constant.getTextRepresentation());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(IotDBColumnReference column) {
        String columnName = column.getColumn().getName();
        if (isAbstractExpression && !columnName.equalsIgnoreCase(IotDBValueStateConstant.TIME_FIELD.getValue()))
            columnName = GlobalConstant.COLUMN_NAME;
        sb.append(columnName);
    }

    public void visitDoubleValueLeft(IotDBConstant constant) {
        // TODO Constant <= Column 逻辑错误已汇报
        sb.append(" >= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .subtract(BigDecimal.valueOf(Math.pow(10, -IotDBConstant.IotDBDoubleConstant.scale)))
                    .setScale(IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    public void visitDoubleValueRight(IotDBConstant constant) {
        sb.append(" <= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .add(BigDecimal.valueOf(Math.pow(10, -IotDBConstant.IotDBDoubleConstant.scale)))
                    .setScale(IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(IotDBBinaryLogicalOperation op) {
        // TODO IotDBBinaryLogicalOperation不支持两边都是常量
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
    public void visit(IotDBSelect s) {
        sb.append("SELECT ");
        if (s.getFetchColumns() == null && s.getCastColumns() == null) {
            sb.append("*");
        } else {
            // cast column
            if (!ObjectUtils.isEmpty(s.getCastColumns())) {
                for (int i = 0; i < s.getCastColumns().size(); i++) {
                    visit(s.getCastColumns().get(i));
                    sb.append(" AS ");
                    sb.append("ref");
                    sb.append(ref++);
                    sb.append(", ");
                }
            }

            // fetch column
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                visit(s.getFetchColumns().get(i));
                // IotDB does not allow duplicate column names
                sb.append(" AS ");
                sb.append("ref");
                sb.append(ref++);
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }

        if (s.getWhereClause() != null) {
            IotDBExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && s.getGroupByExpressions().size() > 0) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<IotDBExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }

        if (!ObjectUtils.isEmpty(s.getIntervalValues())) {
            sb.append(" GROUP BY (");
            sb.append("[").append(s.getIntervalValues().get(0))
                    .append(", ").append(s.getIntervalValues().get(1)).append("),")
                    .append(s.getIntervalValues().get(s.getIntervalValues().size() - 1));
            if (!StringUtils.isBlank(s.getSlidingValue())) sb.append(",").append(s.getSlidingValue());
            sb.append(") ");
        }

        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            List<IotDBExpression> orderBys = s.getOrderByExpressions();
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
    public void visit(IotDBBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(IotDBCastOperation op) {
        sb.append(" CAST(");
        visit(op.getExpr());
        sb.append(" as ");
        sb.append(op.getCastType());
        sb.append(")");
    }

    @Override
    public void visit(IotDBBinaryArithmeticOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(IotDBOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == IotDBOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(IotDBUnaryPostfixOperation op) {
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

    @Override
    public void visit(IotDBInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        if (Randomly.getBoolean()) sb.append(" IN ");
        else sb.append(" CONTAINS ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(IotDBBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(IotDBUnaryPrefixOperation unaryOperation) {
        super.visit((UnaryOperation<IotDBExpression>) unaryOperation);
    }

    @Override
    public void visit(IotDBUnaryNotPrefixOperation unaryOperation) {
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator op = ((IotDBUnaryNotPrefixOperation) unaryOperation).getOp();
        // NOT DOUBLE
        if (op.equals(IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator.NOT_DOUBLE)) {
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
            visit(unaryOperation.getExpression());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            visitDoubleValueLeft(unaryOperation.getExpression().getExpectedValue());
            sb.append(" AND ");
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
            visit(unaryOperation.getExpression());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            visitDoubleValueRight(unaryOperation.getExpression().getExpectedValue());
            if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
            return;
        }

        // NOT BOOLEAN
        if (op.equals(IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator.NOT))
            sb.append(unaryOperation.getOperatorRepresentation());
        else {
            // NOT INT
            visit(unaryOperation.getExpression().getExpectedValue());
            // 子节点为常量、列、强转运算时, NOT符号改为 =
            if (unaryOperation.getExpression() instanceof IotDBConstant
                    || unaryOperation.getExpression() instanceof IotDBColumnReference
                    || unaryOperation.getExpression() instanceof IotDBCastOperation
                    || unaryOperation.getExpression() instanceof IotDBUnaryPrefixOperation
                    || unaryOperation.getExpression() instanceof IotDBBinaryArithmeticOperation)
                sb.append(" = ");
        }

        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        visit(unaryOperation.getExpression());
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
    }

    @Override
    public void visit(UnaryOperation<IotDBExpression> unaryOperation) {
        // Unary NOT Prefix Operation
        if (unaryOperation instanceof IotDBUnaryNotPrefixOperation)
            visit((IotDBUnaryNotPrefixOperation) unaryOperation);
        else if (unaryOperation instanceof IotDBUnaryPrefixOperation)
            visit((IotDBUnaryPrefixOperation) unaryOperation);
    }

}
