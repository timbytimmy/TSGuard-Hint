package com.fuzzy.griddb;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.ConstraintValue;
import com.fuzzy.common.tsaf.ConstraintValueGenerator;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.common.visitor.ToStringVisitor;
import com.fuzzy.common.visitor.UnaryOperation;
import com.fuzzy.griddb.ast.*;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;
import java.util.Stack;

@Slf4j
public class GridDBToConstraintVisitor extends ToStringVisitor<GridDBExpression> implements GridDBVisitor {

    Stack<ConstraintValue> constraintStack = new Stack<>();
    TimeSeriesConstraint nullValueTimestamps;
    String databaseName;
    String tableName;

    public GridDBToConstraintVisitor(String databaseName, String tableName, Set<Long> nullValuesSet) {
        this.databaseName = databaseName;
        this.tableName = tableName;

        TimeSeriesConstraint nullValueTimestamps = new TimeSeriesConstraint(GridDBConstantString.TIME_FIELD_NAME.getName(),
                new RangeConstraint());
        nullValueTimestamps.getRangeConstraints().clear();
        nullValuesSet.forEach(timestamp -> nullValueTimestamps.addEqualValue(new BigDecimal(timestamp)));
        this.nullValueTimestamps = nullValueTimestamps;
    }

    @Override
    public void visitSpecific(GridDBExpression expr) {
        GridDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(GridDBTableReference ref) {

    }

    @Override
    public void visit(GridDBSchemaReference ref) {

    }

    @Override
    public void visit(GridDBConstant constant) {
        constraintStack.add(ConstraintValueGenerator.genConstant(constant.getTextRepresentation()));
    }

    @Override
    public void visit(GridDBColumnReference timeSeries) {
        constraintStack.add(ConstraintValueGenerator.genTimeSeries(timeSeries.getColumn().getName()));
    }

    @Override
    public void visit(UnaryOperation<GridDBExpression> unaryOperation) {
        if (unaryOperation instanceof GridDBUnaryNotPrefixOperation)
            visit((GridDBUnaryNotPrefixOperation) unaryOperation);

        if (unaryOperation instanceof GridDBUnaryPrefixOperation)
            visit((GridDBUnaryPrefixOperation) unaryOperation);
    }

    @Override
    public void visit(GridDBBinaryLogicalOperation op) {
        constraintStack.add(ConstraintValueGenerator.genOperator(op.getTextRepresentation()));
        visit(op.getLeft());
        visit(op.getRight());
        // 将结果存储, 弹出运算符
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();
        ConstraintValue logicOp = constraintStack.pop();
        // 仅支持列约束和列约束进行运算
        assert left instanceof ConstraintValue.TimeSeriesConstraintValue
                && right instanceof ConstraintValue.TimeSeriesConstraintValue;
        ConstraintValue baseTimeSeriesConstraintLeft = left.transToBaseSeq(databaseName, tableName);
        ConstraintValue baseTimeSeriesConstraintRight = right.transToBaseSeq(databaseName, tableName);

        // 全部转为baseTimeSeries
        if (GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND ==
                GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.getOperatorByText(logicOp.getOperator())) {
            TimeSeriesConstraint baseTimeSeriesConstraint = baseTimeSeriesConstraintLeft.getTimeSeriesConstraint()
                    .intersects(baseTimeSeriesConstraintRight.getTimeSeriesConstraint());
            constraintStack.add(ConstraintValueGenerator.genConstraint(baseTimeSeriesConstraintLeft.getTimeSeriesValue(),
                    baseTimeSeriesConstraint));
        } else {
            TimeSeriesConstraint baseTimeSeriesConstraint = baseTimeSeriesConstraintLeft.getTimeSeriesConstraint()
                    .union(baseTimeSeriesConstraintRight.getTimeSeriesConstraint(),
                            GridDBConstant.createFloatArithmeticTolerance());
            baseTimeSeriesConstraint.setTimeSeriesName(GlobalConstant.BASE_TIME_SERIES_NAME);
            constraintStack.add(ConstraintValueGenerator.genConstraint(baseTimeSeriesConstraintLeft.getTimeSeriesValue(),
                    baseTimeSeriesConstraint));
        }
    }

    @Override
    public void visit(GridDBSelect s) {

    }

    @Override
    public void visit(GridDBBinaryComparisonOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();

        if (left.isConstant() && right.isConstant()) {
            // 值 and 值 比较
            GridDBConstant expectedValue = op.getExpectedValue();
            if (expectedValue.asBooleanNotNull())
                constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
            else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
            return;
        }

        ConstraintValue.TimeSeriesValue timeSeriesValue;
        BigDecimal constantValue;
        GridDBBinaryComparisonOperation.BinaryComparisonOperator operator = op.getOp();
        // 格式均调整为 左边列 右边值
        if (left.isTimeSeries() && right.isConstant()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) left;
            constantValue = new BigDecimal(right.getConstant());
        } else if (left.isConstant() && right.isTimeSeries()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) right;
            operator = operator.reverseInequality();
            constantValue = new BigDecimal(left.getConstant());
        } else if (left.isTimeSeries() && right.isTimeSeries()) {
            // 列 and 列 比较 (列相同) -> 交换位置
            ConstraintValue.TimeSeriesValue leftTimeSeriesValue = (ConstraintValue.TimeSeriesValue)
                    left.transToBaseSeq(databaseName, tableName);
            ConstraintValue.TimeSeriesValue rightTimeSeriesValue = (ConstraintValue.TimeSeriesValue)
                    right.transToBaseSeq(databaseName, tableName);
            leftTimeSeriesValue.subFactor(rightTimeSeriesValue.getFactor());
            rightTimeSeriesValue.subIntercept(leftTimeSeriesValue.getIntercept());
            // 消除timeSeries
            timeSeriesValue = leftTimeSeriesValue;
            timeSeriesValue.setInterceptZero();
            constantValue = rightTimeSeriesValue.getIntercept().getBigDecimalConstant();
        } else if (left.isTimeSeriesConstraint() && right.isTimeSeriesConstraint()) {
            // 列约束参与比较运算(BOOL值)
            compareTimeSeriesConstraint(left.transToBaseSeq(databaseName, tableName).getTimeSeriesConstraint(),
                    right.transToBaseSeq(databaseName, tableName).getTimeSeriesConstraint(), operator);
            return;
        } else {
            throw new AssertionError();
        }

        // 列系数为0值, 直接比较常量截距 (系数为0后)
        if (timeSeriesValue.getFactor().getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0) {
            if (operator.getExpectedValue(GridDBConstant.createBigDecimalConstant(
                            timeSeriesValue.getIntercept().getBigDecimalConstant()),
                    GridDBConstant.createBigDecimalConstant(constantValue)).asBooleanNotNull())
                constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
            else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
            return;
        }
        // TODO 非常量列因子待实现
        // 消除列因子: 转换常量值, 比较运算符方向
        if (timeSeriesValue.hasNegativeFactor()) operator = operator.reverseInequality();
        constantValue = timeSeriesValue.eliminateFactor(constantValue);

        String timeSeriesName = timeSeriesValue.getTimeSeriesName();
        TimeSeriesConstraint curConstraint;
        switch (operator) {
            case EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, new RangeConstraint(constantValue, constantValue));
                break;
            case NOT_EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName);
                curConstraint.addNotEqualValue(constantValue);
                break;
            case LESS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(new BigDecimal(Long.MIN_VALUE))
                        .lessEqualValue(constantValue).build());
                curConstraint.addNotEqualValue(constantValue);
                break;
            case LESS_EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(new BigDecimal(Long.MIN_VALUE))
                        .lessEqualValue(constantValue).build());
                break;
            case GREATER:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(constantValue)
                        .lessEqualValue(new BigDecimal(Long.MAX_VALUE)).build());
                curConstraint.addNotEqualValue(constantValue);
                break;
            case GREATER_EQUALS:
                curConstraint = new TimeSeriesConstraint(timeSeriesName, RangeConstraint.builder()
                        .greaterEqualValue(constantValue)
                        .lessEqualValue(new BigDecimal(Long.MAX_VALUE)).build());
                break;
            default:
                throw new AssertionError();
        }
        // 生成单个约束条件
        constraintStack.add(ConstraintValueGenerator.genConstraint(timeSeriesValue, curConstraint));
    }

    private void compareTimeSeriesConstraint(TimeSeriesConstraint left, TimeSeriesConstraint right,
                                             GridDBBinaryComparisonOperation.BinaryComparisonOperator operator) {
        TimeSeriesConstraint leftTrue = left, leftFalse = left.complement();
        TimeSeriesConstraint rightTrue = right, rightFalse = right.complement();
        TimeSeriesConstraint curConstraint;
        switch (operator) {
            case EQUALS:
                // TRUE AND TRUE || FALSE AND FALSE
                curConstraint = leftTrue.intersects(rightTrue).union(leftFalse.intersects(rightFalse),
                        GridDBConstant.createFloatArithmeticTolerance());
                break;
            case NOT_EQUALS:
                // TRUE AND FALSE || FALSE AND TRUE
                curConstraint = leftTrue.intersects(rightFalse).union(leftFalse.intersects(rightTrue),
                        GridDBConstant.createFloatArithmeticTolerance());
                break;
            case LESS:
                // FALSE AND TRUE
                curConstraint = leftFalse.intersects(rightTrue);
                break;
            case LESS_EQUALS:
                // TRUE AND TRUE || FALSE AND ALL
                curConstraint = leftTrue.intersects(rightTrue).union(leftFalse,
                        GridDBConstant.createFloatArithmeticTolerance());
                break;
            case GREATER:
                // TRUE AND FALSE
                curConstraint = leftTrue.intersects(rightFalse);
                break;
            case GREATER_EQUALS:
                // TRUE AND ALL || FALSE AND FALSE
                curConstraint = leftTrue.union(leftFalse.intersects(rightFalse),
                        GridDBConstant.createFloatArithmeticTolerance());
                break;
            default:
                throw new AssertionError();
        }
        // 生成单个约束条件
        constraintStack.add(ConstraintValueGenerator.genConstraint(ConstraintValueGenerator.genBaseTimeSeries(),
                curConstraint));
    }

    @Override
    public void visit(GridDBCastOperation op) {
        visit(op.getExpr());
        ConstraintValue exprValue = constraintStack.pop();
        // 其他类型强转均满足 不损失精度的转换, 故TSQS中强转不会对Y轴增加额外的约束条件
        if (exprValue.isTimeSeries() && op.getCastType().equalsIgnoreCase(GridDBCastOperation.CastType.BOOL.getType())) {
            // 列强转BOOL值 -> 约束列不为0值(GridDB 非0值转为TRUE)
            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(exprValue.getTimeSeriesName());
            timeSeriesConstraint.addNotEqualValue(BigDecimal.ZERO);
            constraintStack.add(ConstraintValueGenerator.genConstraint(exprValue.getTimeSeriesValue(),
                    timeSeriesConstraint));
        } else constraintStack.add(exprValue);
    }

    @Override
    public void visit(GridDBBinaryOperation op) {
        // TODO
        log.warn("GridDBBinaryOperation");
        throw new IgnoreMeException();
    }

    @Override
    public void visit(GridDBUnaryPrefixOperation op) {
        visit(op.getExpression());
        ConstraintValue constraintValue = constraintStack.pop();
        // 约束条件取反
        if (op.getOp() == GridDBUnaryPrefixOperation.GridDBUnaryPrefixOperator.MINUS)
            constraintStack.add(constraintValue.negate());
        else
            constraintStack.add(constraintValue);
    }

    @Override
    public void visit(GridDBUnaryNotPrefixOperation op) {
        // Unary NOT Prefix Operation
        visit(op.getExpression());
        ConstraintValue constraintValue = constraintStack.pop();
        if (constraintValue.isTimeSeries()) {
            // 列表达式系数被减法清除 => NOT 常量 => TRUE
            ConstraintValue.TimeSeriesValue timeSeriesValue = (ConstraintValue.TimeSeriesValue) constraintValue;
            if (timeSeriesValue.getFactor().getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0) {
                if (op.getExpression().getExpectedValue().castAs(GridDBSchema.GridDBDataType.BIGDECIMAL)
                        .getBigDecimalValue().compareTo(timeSeriesValue.getIntercept().getBigDecimalConstant()) == 0)
                    constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
                else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
                return;
            }

            // 列预期值, (NOT timeSeriesValue) 即将其预期值取TRUE
            BigDecimal expectedValue = op.getExpression().getExpectedValue().castAs(
                    GridDBSchema.GridDBDataType.BIGDECIMAL).getBigDecimalValue();
            // 消除列因子
            expectedValue = timeSeriesValue.eliminateFactor(expectedValue);
            BigDecimal lessValue = expectedValue;
            BigDecimal greaterValue = expectedValue;
            // 浮点运算存在精度损失
            if (BigDecimalUtil.isDouble(expectedValue)) {
                greaterValue = expectedValue.subtract(
                        BigDecimal.valueOf(Math.pow(10, -GridDBConstant.GridDBDoubleConstant.scale)));
                lessValue = expectedValue.add(
                        BigDecimal.valueOf(Math.pow(10, -GridDBConstant.GridDBDoubleConstant.scale)));
            }
            TimeSeriesConstraint curConstraint = new TimeSeriesConstraint(timeSeriesValue.getTimeSeriesName(), new RangeConstraint(
                    greaterValue, lessValue));
            constraintStack.add(ConstraintValueGenerator.genConstraint(timeSeriesValue, curConstraint));
        } else if (constraintValue.isConstant()) {
            // TRUE -> 常量覆盖区间所有范围
            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
        } else if (constraintValue.isTimeSeriesConstraint()) {
            // 列约束取反
            constraintStack.add(constraintValue.negate());
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void visit(GridDBBinaryArithmeticOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();

        GridDBBinaryArithmeticOperation.GridDBBinaryArithmeticOperator operator = op.getOp();
        // 列 op 列
        if (left.isTimeSeries() && right.isTimeSeries()) {
            // 转换为相同的基序列
            ConstraintValue.TimeSeriesValue timeSeriesValueLeft = (ConstraintValue.TimeSeriesValue)
                    left.transToBaseSeq(databaseName, tableName);
            ConstraintValue.TimeSeriesValue timeSeriesValueRight = (ConstraintValue.TimeSeriesValue)
                    right.transToBaseSeq(databaseName, tableName);
            // 因子系数和截距相加减
            switch (operator) {
                case PLUS:
                    timeSeriesValueLeft.addFactor(timeSeriesValueRight.getFactor());
                    timeSeriesValueLeft.addIntercept(timeSeriesValueRight.getIntercept());
                    break;
                case SUBTRACT:
                    timeSeriesValueLeft.subFactor(timeSeriesValueRight.getFactor());
                    timeSeriesValueLeft.subIntercept(timeSeriesValueRight.getIntercept());
                    break;
                case MULTIPLY:
                case DIVIDE:
                case MODULO:
                    // TODO 暂不支持列和列进行乘除操作
                    log.warn("暂不支持列和列进行乘、除、求模操作.");
                    throw new IgnoreMeException();
                default:
                    throw new AssertionError();
            }
            constraintStack.add(timeSeriesValueLeft);
            return;
        }

        // 常量 op 常量
        if (left.isConstant() && right.isConstant()) {
            visit(op.getExpectedValue());
            return;
        }

        // 列 op 常量 / 常量 op 列
        ConstraintValue.TimeSeriesValue timeSeriesValue;
        ConstraintValue.ConstantValue constantValue;
        if (left.isConstant() && right.isTimeSeries()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) right;
            constantValue = (ConstraintValue.ConstantValue) left;
            // 常量 / 列
            if (operator == GridDBBinaryArithmeticOperation.GridDBBinaryArithmeticOperator.DIVIDE) {
                timeSeriesValue.transToReciprocalVal(constantValue);
                constraintStack.add(timeSeriesValue);
                return;
            }
            // 针对列的减法 => 列约束条件取反, 变为加法
            if (operator == GridDBBinaryArithmeticOperation.GridDBBinaryArithmeticOperator.SUBTRACT) {
                timeSeriesValue = timeSeriesValue.negate();
                operator = GridDBBinaryArithmeticOperation.GridDBBinaryArithmeticOperator.PLUS;
            }
        } else if (left.isTimeSeries() && right.isConstant()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) left;
            constantValue = (ConstraintValue.ConstantValue) right;
        } else {
            throw new AssertionError();
        }

        // 将常量作为倍率因子附加于first中
        switch (operator) {
            case PLUS:
                timeSeriesValue.addIntercept(constantValue);
                break;
            case SUBTRACT:
                timeSeriesValue.subIntercept(constantValue);
                break;
            case MULTIPLY:
                timeSeriesValue.multiplyFactor(constantValue);
                break;
            case DIVIDE:
                constantValue = ConstraintValueGenerator.genConstant(new BigDecimal(1)
                        .divide(constantValue.getBigDecimalConstant(), DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP)
                        .toPlainString());
                timeSeriesValue.multiplyFactor(constantValue);
                break;
            case MODULO:
                log.warn("不支持MODULO");
                throw new IgnoreMeException();
            default:
                throw new AssertionError();
        }
        constraintStack.add(timeSeriesValue);
    }

    @Override
    public void visit(GridDBOrderByTerm op) {
        // TODO
    }

    @Override
    public void visit(GridDBUnaryPostfixOperation op) {
        visit(op.getExpression());
        ConstraintValue exprValue = constraintStack.pop();

        if (exprValue.isConstant() && !op.isNegated())
            constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
        else if (exprValue.isConstant() && op.isNegated())
            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
        else if (exprValue.isTimeSeries() && !op.isNegated()) {
            // time series is null -> timestamp list
            constraintStack.add(ConstraintValueGenerator.genConstraint(
                    ConstraintValueGenerator.genTimeSeries(GridDBConstantString.TIME_FIELD_NAME.getName()),
                    nullValueTimestamps));
        } else if (exprValue.isTimeSeries() && op.isNegated()) {
            // 将timeSeriesConstraint因子截距设为0
            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(exprValue.getTimeSeriesName(),
                    new RangeConstraint());
            constraintStack.add(ConstraintValueGenerator.genConstraint(exprValue.getTimeSeriesValue(),
                    timeSeriesConstraint));
        } else if (exprValue.isTimeSeriesConstraint() && !op.isNegated()) {
            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(GlobalConstant.BASE_TIME_SERIES_NAME,
                    new RangeConstraint());
            timeSeriesConstraint.getRangeConstraints().clear();
            constraintStack.add(ConstraintValueGenerator.genConstraint(ConstraintValueGenerator.genBaseTimeSeries(),
                    timeSeriesConstraint));
        } else if (exprValue.isTimeSeriesConstraint() && op.isNegated()) {
            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void visit(GridDBInOperation op) {
        if (op.getListElements().isEmpty()) throw new AssertionError();

        // 将IN转换为OR表达式组合
        GridDBBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator;
        GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator logicalOperator;
        if (!op.isTrue()) {
            comparisonOperator = GridDBBinaryComparisonOperation.BinaryComparisonOperator.NOT_EQUALS;
            logicalOperator = GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND;
        } else {
            comparisonOperator = GridDBBinaryComparisonOperation.BinaryComparisonOperator.EQUALS;
            logicalOperator = GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.OR;
        }

        int index = 0;
        GridDBExpression combinationExpr = new GridDBBinaryComparisonOperation(op.getExpr(),
                op.getListElements().get(index++), comparisonOperator);

        for (; index < op.getListElements().size(); index++) {
            GridDBBinaryComparisonOperation right = new GridDBBinaryComparisonOperation(op.getExpr(),
                    op.getListElements().get(index), comparisonOperator);
            combinationExpr = new GridDBBinaryLogicalOperation(combinationExpr, right, logicalOperator);
        }
        visit(combinationExpr);
    }

    @Override
    public void visit(GridDBBetweenOperation op) {
        // 将BETWEEN转换为LESS_EQUALS
        GridDBBinaryComparisonOperation left = new GridDBBinaryComparisonOperation(op.getLeft(), op.getExpr(),
                GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        GridDBBinaryComparisonOperation right = new GridDBBinaryComparisonOperation(op.getExpr(), op.getRight(),
                GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        visit(new GridDBBinaryLogicalOperation(left, right,
                GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND));
    }

    @Override
    public void visit(GridDBComputableFunction f) {
        // TODO 仅支持常量
        visit(f.getExpectedValue());
    }
}
