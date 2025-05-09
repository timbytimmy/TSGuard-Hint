package com.fuzzy.griddb;

import com.fuzzy.Randomly;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.util.TimeUtil;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.griddb.ast.*;
import com.fuzzy.griddb.ast.GridDBOrderByTerm.GridDBOrder;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import com.jdcloud.sdk.utils.StringUtils;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class GridDBToStringVisitor extends ToStringVisitor<GridDBExpression> implements GridDBVisitor {

    // 是否抽象语法节点
    boolean isAbstractExpression;

    public GridDBToStringVisitor() {
        isAbstractExpression = false;
    }

    public GridDBToStringVisitor(boolean isAbstractExpression) {
        this.isAbstractExpression = isAbstractExpression;
    }

    @Override
    public void visitSpecific(GridDBExpression expr) {
        GridDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(GridDBTableReference ref) {
        if (!isAbstractExpression) sb.append(ref.getTable().getName());
        else sb.append(GlobalConstant.TABLE_NAME);
    }

    @Override
    public void visit(GridDBSchemaReference ref) {
        if (!isAbstractExpression) sb.append(ref.getSchema().getRandomTable().getDatabaseName());
        else sb.append(GlobalConstant.DATABASE_NAME);
    }

    @Override
    public void visit(GridDBConstant constant) {
        if (!isAbstractExpression) {
            String textRepresentation = constant.getTextRepresentation();
            if (constant.isTimestamp())
                textRepresentation = "TIMESTAMP('" +
                        TimeUtil.timestampToISO8601(Long.valueOf(constant.getTextRepresentation())) + "')";
            sb.append(textRepresentation);
        } else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(GridDBColumnReference column) {
        String columnName = column.getColumn().getName();
        if (isAbstractExpression && !columnName.equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName()))
            columnName = GlobalConstant.COLUMN_NAME;
        sb.append(columnName);
    }

    public void visitDoubleValueLeft(GridDBConstant constant) {
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .subtract(BigDecimal.valueOf(Math.pow(10, -GridDBConstant.GridDBDoubleConstant.scale)))
                    .setScale(GridDBConstant.GridDBDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
        sb.append(" <= ");
    }

    public void visitDoubleValueRight(GridDBConstant constant) {
        sb.append(" <= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .add(BigDecimal.valueOf(Math.pow(10, -GridDBConstant.GridDBDoubleConstant.scale)))
                    .setScale(GridDBConstant.GridDBDoubleConstant.scale, RoundingMode.HALF_UP)
                    .toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(GridDBBinaryLogicalOperation op) {
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
    public void visit(GridDBSelect s) {
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
                sb.append(Randomly.fromOptions("ALL ", ""));
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
            GridDBExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && s.getGroupByExpressions().size() > 0) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<GridDBExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }

        if (!ObjectUtils.isEmpty(s.getIntervalValues())) {
            sb.append(" GROUP BY RANGE(").append(GridDBConstantString.TIME_FIELD_NAME.getName()).append(")");
            sb.append(" EVERY (");
            for (int i = 0; i < s.getIntervalValues().size(); i++) {
                sb.append(s.getIntervalValues().get(i));
                if (i == 0) sb.append(", SECOND");
                if (i != s.getIntervalValues().size() - 1) sb.append(", ");

            }
            sb.append(") FILL(NONE)");
        }

        if (!StringUtils.isBlank(s.getSlidingValue())) {
            sb.append(" SLIDING (")
                    .append(s.getSlidingValue())
                    .append(")");
        }

        if (!ObjectUtils.isEmpty(s.getOrderByExpressions())) {
            sb.append(" ORDER BY ");
            List<GridDBExpression> orderBys = s.getOrderByExpressions();
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
    public void visit(GridDBBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GridDBCastOperation op) {
        sb.append(" CAST(");
        visit(op.getExpr());
        sb.append(" as ");
        sb.append(op.getCastType());
        sb.append(")");
    }

    @Override
    public void visit(GridDBBinaryOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GridDBUnaryPrefixOperation op) {
        super.visit((UnaryOperation<GridDBExpression>) op);
    }

    @Override
    public void visit(GridDBUnaryNotPrefixOperation unaryOperation) {
        // Unary NOT Prefix Operation
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        GridDBUnaryNotPrefixOperation.GridDBUnaryNotPrefixOperator op =
                ((GridDBUnaryNotPrefixOperation) unaryOperation).getOp();
        // NOT DOUBLE
        if (op.equals(GridDBUnaryNotPrefixOperation.GridDBUnaryNotPrefixOperator.NOT_DOUBLE)) {
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
        if (op.equals(GridDBUnaryNotPrefixOperation.GridDBUnaryNotPrefixOperator.NOT))
            sb.append(unaryOperation.getOperatorRepresentation());
        else {
            // NOT INT
            visit(unaryOperation.getExpression().getExpectedValue());
            // 子节点为常量、列、强转运算时, NOT符号改为 =
            if (unaryOperation.getExpression() instanceof GridDBConstant
                    || unaryOperation.getExpression() instanceof GridDBColumnReference
                    || unaryOperation.getExpression() instanceof GridDBCastOperation
                    || unaryOperation.getExpression() instanceof GridDBUnaryPrefixOperation
                    || unaryOperation.getExpression() instanceof GridDBBinaryArithmeticOperation
                    || unaryOperation.getExpression() instanceof GridDBComputableFunction
                    || unaryOperation.getExpression() instanceof GridDBBinaryOperation)
                sb.append(" = ");
        }

        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        visit(unaryOperation.getExpression());
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
    }

    @Override
    public void visit(GridDBBinaryArithmeticOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(GridDBOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == GridDBOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(GridDBUnaryPostfixOperation op) {
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
    public void visit(GridDBComputableFunction f) {
        sb.append(f.getFunction().getName());
        sb.append("(");
        for (int i = 0; i < f.getArguments().length; i++) {
            if (i != 0) sb.append(", ");
            visit(f.getArguments()[i]);
        }
        sb.append(")");
    }

    @Override
    public void visit(GridDBInOperation op) {
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
    public void visit(GridDBBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(UnaryOperation<GridDBExpression> unaryOperation) {
        if (unaryOperation instanceof GridDBUnaryNotPrefixOperation)
            visit((GridDBUnaryNotPrefixOperation) unaryOperation);
        else if (unaryOperation instanceof GridDBUnaryPrefixOperation)
            visit((GridDBUnaryPrefixOperation) unaryOperation);
    }

}
