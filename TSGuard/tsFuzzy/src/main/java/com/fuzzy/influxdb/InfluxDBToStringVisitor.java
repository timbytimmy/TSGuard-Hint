package com.fuzzy.influxdb;

import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.util.TimeUtil;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.influxdb.ast.*;
import com.fuzzy.influxdb.ast.InfluxDBConstant.InfluxDBDoubleConstant;
import com.fuzzy.influxdb.ast.InfluxDBOrderByTerm.InfluxDBOrder;
import com.fuzzy.influxdb.ast.InfluxDBUnaryNotPrefixOperation.InfluxDBUnaryNotPrefixOperator;
import com.fuzzy.influxdb.util.InfluxDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Slf4j
public class InfluxDBToStringVisitor extends ToStringVisitor<InfluxDBExpression> implements InfluxDBVisitor {

    // 是否抽象语法节点
    boolean isAbstractExpression;

    public InfluxDBToStringVisitor() {
        this.isAbstractExpression = false;
    }

    public InfluxDBToStringVisitor(boolean isAbstractExpression) {
        this.isAbstractExpression = isAbstractExpression;
    }

    @Override
    public void visitSpecific(InfluxDBExpression expr) {
        InfluxDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(InfluxDBTableReference ref) {
        if (!isAbstractExpression) sb.append(ref.getTable().getFullName());
        else sb.append(GlobalConstant.TABLE_NAME);
    }

    @Override
    public void visit(InfluxDBConstant constant) {
        if (!isAbstractExpression) {
            String textRepresentation = constant.getTextRepresentation();
            if (constant.isTimestamp())
                textRepresentation = TimeUtil.timestampToRFC3339(Long.valueOf(constant.getTextRepresentation()));
            sb.append(textRepresentation);
        } else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(InfluxDBColumnReference column) {
        // TODO getFullQualifiedName
        String columnName = column.getColumn().getName();
        if (isAbstractExpression && !columnName.equalsIgnoreCase(InfluxDBValueStateConstant.TIME_FIELD.getValue()))
            columnName = GlobalConstant.COLUMN_NAME;
        sb.append(columnName);
    }

    public void visitDoubleValueLeft(InfluxDBConstant constant) {
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .subtract(BigDecimal.valueOf(Math.pow(10, -InfluxDBDoubleConstant.scale)))
                    .setScale(InfluxDBDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
        sb.append(" <= ");
    }

    public void visitDoubleValueRight(InfluxDBConstant constant) {
        sb.append(" <= ");
        if (!isAbstractExpression)
            sb.append(BigDecimal.valueOf(constant.isDouble() ? constant.getDouble() :
                            constant.getBigDecimalValue().doubleValue())
                    .add(BigDecimal.valueOf(Math.pow(10, -InfluxDBDoubleConstant.scale)))
                    .setScale(InfluxDBDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString());
        else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    public void visitIntExpectedValue(InfluxDBConstant constant) {
        if (!isAbstractExpression) {
            // INT UINT类型不支持常量/字段值比较时常量具备后缀
            if (constant.isInt()) sb.append(constant.getInt());
            else sb.append(constant);
        } else sb.append(GlobalConstant.CONSTANT_NAME);
    }

    @Override
    public void visit(InfluxDBBinaryLogicalOperation op) {
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
    public void visit(InfluxDBSelect s) {
        sb.append("q=SELECT ");
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                // 随机使用Cast转换 -> 已存在CastOperation, 故此处忽略
                InfluxDBColumnReference fetchColumnReference = (InfluxDBColumnReference) s.getFetchColumns().get(i);
                visit(fetchColumnReference);
                sb.append(" AS ").append(InfluxDBValueStateConstant.REF.getValue()).append(i);
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
            InfluxDBExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && s.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            List<InfluxDBExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }
        if (!ObjectUtils.isEmpty(s.getIntervalValues())) {
            sb.append(" GROUP BY time(");
            for (int i = 0; i < s.getIntervalValues().size(); i++) {
                sb.append(s.getIntervalValues().get(i)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1).append(")");
        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            List<InfluxDBExpression> orderBys = s.getOrderByExpressions();
            for (int i = 0; i < orderBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                //visit(s.getOrderByExpressions().get(i));
                visit(orderBys.get(i));
            }
        } else {
            sb.append(" ORDER BY time ASC");
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
    public void visit(InfluxDBBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(InfluxDBCastOperation op) {
        visit(op.getExpr());
        sb.append("::");
        sb.append(op.getCastType());
    }

    @Override
    public void visit(InfluxDBBinaryBitwiseOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

//    @Override
//    public void visit(InfluxDBBinaryArithmeticOperation op) {
//        sb.append("(");
//        visit(op.getLeft());
//        sb.append(") ");
//        sb.append(op.getOp().getTextRepresentation());
//        sb.append(" (");
//        visit(op.getRight());
//        sb.append(")");
//    }

    @Override
    public void visit(InfluxDBBinaryArithmeticOperation op) {
        // get rid of “(const + const)” and “(const - const)” so we never get “(1234)+(5678)”
        if (!isAbstractExpression
                && op.getLeft()  instanceof InfluxDBConstant
                && op.getRight() instanceof InfluxDBConstant) {
            InfluxDBConstant l = (InfluxDBConstant) op.getLeft();
            InfluxDBConstant r = (InfluxDBConstant) op.getRight();
            String sym = op.getOp().getTextRepresentation();
            try {
                java.math.BigDecimal lv = l.getBigDecimalValue();
                java.math.BigDecimal rv = r.getBigDecimalValue();
                java.math.BigDecimal res;
                if ("+".equals(sym)) {
                    res = lv.add(rv);
                } else if ("-".equals(sym)) {
                    res = lv.subtract(rv);
                } else {
                    throw new IllegalArgumentException("unsupported fold: " + sym);
                }
                // drop trailing “.0” on integers
                String out = res.stripTrailingZeros().toPlainString();
                sb.append(out);
                return;
            } catch (Exception e) {
                // fallback to normal printing
            }
        }
        // non‐constant or other ops: fall back
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ")
                .append(op.getOp().getTextRepresentation())
                .append(" (");
        visit(op.getRight());
        sb.append(")");
    }


    @Override
    public void visit(InfluxDBOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == InfluxDBOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(InfluxDBStringExpression op) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(InfluxDBUnaryPrefixOperation unaryOperation) {
        super.visit((UnaryOperation<InfluxDBExpression>) unaryOperation);
    }

    @Override
    public void visit(InfluxDBUnaryNotPrefixOperation unaryOperation) {
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        InfluxDBUnaryNotPrefixOperator op = unaryOperation.getOp();
        // Not Float
        if (op.equals(InfluxDBUnaryNotPrefixOperator.NOT_FLOAT)) {
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

        // NOT Boolean
        if (op.equals(InfluxDBUnaryNotPrefixOperator.NOT_BOOLEAN))
            sb.append(unaryOperation.getOperatorRepresentation());
        else {
            // NOT_UINTEGER/NOT_STR
            visitIntExpectedValue(unaryOperation.getExpression().getExpectedValue());
            // 子节点为常量、列、强转运算时, NOT符号改为 =
            if (unaryOperation.getExpression() instanceof InfluxDBConstant
                    || unaryOperation.getExpression() instanceof InfluxDBColumnReference
                    || unaryOperation.getExpression() instanceof InfluxDBCastOperation
                    || unaryOperation.getExpression() instanceof InfluxDBBinaryBitwiseOperation
                    || unaryOperation.getExpression() instanceof InfluxDBBinaryArithmeticOperation
                    || unaryOperation.getExpression() instanceof InfluxDBUnaryPrefixOperation)
                sb.append(" = ");
        }

        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append('(');
        visit(unaryOperation.getExpression());
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
        if (!unaryOperation.omitBracketsWhenPrinting()) sb.append(')');
    }

    @Override
    public void visit(UnaryOperation<InfluxDBExpression> unaryOperation) {
        // Unary NOT Prefix Operation
        if (unaryOperation instanceof InfluxDBUnaryNotPrefixOperation)
            visit((InfluxDBUnaryNotPrefixOperation) unaryOperation);
        else if (unaryOperation instanceof InfluxDBUnaryPrefixOperation)
            visit((InfluxDBUnaryPrefixOperation) unaryOperation);
    }
}
