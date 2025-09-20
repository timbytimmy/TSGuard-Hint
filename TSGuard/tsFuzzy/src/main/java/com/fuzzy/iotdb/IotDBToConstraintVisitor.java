package com.fuzzy.iotdb;

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
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.util.IotDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;
import java.util.Stack;

@Slf4j
public class IotDBToConstraintVisitor extends ToStringVisitor<IotDBExpression> implements IotDBVisitor {

    Stack<ConstraintValue> constraintStack = new Stack<>();
    TimeSeriesConstraint nullValueTimestamps;
    String databaseName;
    String tableName;

    public IotDBToConstraintVisitor(String databaseName, String tableName, Set<Long> nullValuesSet) {
        this.databaseName = databaseName;
        this.tableName = tableName;

        TimeSeriesConstraint nullValueTimestamps = new TimeSeriesConstraint(IotDBValueStateConstant.TIME_FIELD.getValue(),
                new RangeConstraint());
        nullValueTimestamps.getRangeConstraints().clear();
        nullValuesSet.forEach(timestamp -> nullValueTimestamps.addEqualValue(new BigDecimal(timestamp)));
        this.nullValueTimestamps = nullValueTimestamps;
    }

    @Override
    public void visitSpecific(IotDBExpression expr) {
        IotDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(IotDBTableReference ref) {

    }

    @Override
    public void visit(IotDBSchemaReference ref) {

    }

    @Override
    public void visit(IotDBConstant constant) {
        constraintStack.add(ConstraintValueGenerator.genConstant(constant.getTextRepresentation()));
    }

    @Override
    public void visit(IotDBColumnReference timeSeries) {
        constraintStack.add(ConstraintValueGenerator.genTimeSeries(timeSeries.getColumn().getName()));
    }

    @Override
    public void visit(UnaryOperation<IotDBExpression> unaryOperation) {
        if (unaryOperation instanceof IotDBUnaryNotPrefixOperation)
            visit((IotDBUnaryNotPrefixOperation) unaryOperation);

        if (unaryOperation instanceof IotDBUnaryPrefixOperation)
            visit((IotDBUnaryPrefixOperation) unaryOperation);
    }

    @Override
    public void visit(IotDBBinaryLogicalOperation op) {
        constraintStack.add(ConstraintValueGenerator.genOperator(op.getTextRepresentation()));
        visit(op.getLeft());
        visit(op.getRight());
        // 将结果存储, 弹出运算符
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();
        ConstraintValue logicOp = constraintStack.pop();
        // 仅支持列约束和列约束进行运算
        if (!(left instanceof ConstraintValue.TimeSeriesConstraintValue
                && right instanceof ConstraintValue.TimeSeriesConstraintValue)) {
            log.error("仅支持列约束和列约束进行运算");
            throw new IgnoreMeException();
        }
        ConstraintValue baseTimeSeriesConstraintLeft = left.transToBaseSeq(databaseName, tableName);
        ConstraintValue baseTimeSeriesConstraintRight = right.transToBaseSeq(databaseName, tableName);

        // 全部转为baseTimeSeries
        if (IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND ==
                IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.getOperatorByText(logicOp.getOperator())) {
            TimeSeriesConstraint baseTimeSeriesConstraint = baseTimeSeriesConstraintLeft.getTimeSeriesConstraint()
                    .intersects(baseTimeSeriesConstraintRight.getTimeSeriesConstraint());
            constraintStack.add(ConstraintValueGenerator.genConstraint(baseTimeSeriesConstraintLeft.getTimeSeriesValue(),
                    baseTimeSeriesConstraint));
        } else {
            TimeSeriesConstraint baseTimeSeriesConstraint = baseTimeSeriesConstraintLeft.getTimeSeriesConstraint()
                    .union(baseTimeSeriesConstraintRight.getTimeSeriesConstraint(),
                            IotDBConstant.createFloatArithmeticTolerance());
            baseTimeSeriesConstraint.setTimeSeriesName(GlobalConstant.BASE_TIME_SERIES_NAME);
            constraintStack.add(ConstraintValueGenerator.genConstraint(baseTimeSeriesConstraintLeft.getTimeSeriesValue(),
                    baseTimeSeriesConstraint));
        }
    }

    @Override
    public void visit(IotDBSelect s) {

    }

    @Override
    public void visit(IotDBBinaryComparisonOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();

        if (left.isConstant() && right.isConstant()) {
            // 值 and 值 比较
            IotDBConstant expectedValue = op.getExpectedValue();
            if (expectedValue.asBooleanNotNull())
                constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
            else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
            return;
        }

        ConstraintValue.TimeSeriesValue timeSeriesValue;
        BigDecimal constantValue;
        IotDBBinaryComparisonOperation.BinaryComparisonOperator operator = op.getOp();
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
            // 消除TimeSeries
            timeSeriesValue = leftTimeSeriesValue;
            timeSeriesValue.setInterceptZero();
            constantValue = rightTimeSeriesValue.getIntercept().getBigDecimalConstant();
        } else if (left.isTimeSeriesConstraint() && right.isTimeSeriesConstraint()) {
            // BOOL仅支持等值比较
            compareTimeSeriesConstraint(left.transToBaseSeq(databaseName, tableName).getTimeSeriesConstraint(),
                    right.transToBaseSeq(databaseName, tableName).getTimeSeriesConstraint(), operator);
            return;
        } else {
            // TODO
            log.warn("单侧TimeSeriesConstraint, 另一侧非BOOL");
            throw new IgnoreMeException();
        }

        // 列系数为0值, 直接比较常量截距 (系数为0后)
        if (timeSeriesValue.getFactor().getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0) {
            if (operator.getExpectedValue(IotDBConstant.createBigDecimalConstant(
                            timeSeriesValue.getIntercept().getBigDecimalConstant()),
                    IotDBConstant.createBigDecimalConstant(constantValue)).asBooleanNotNull())
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
                                             IotDBBinaryComparisonOperation.BinaryComparisonOperator operator) {
        TimeSeriesConstraint leftTrue = left, leftFalse = left.complement();
        TimeSeriesConstraint rightTrue = right, rightFalse = right.complement();
        TimeSeriesConstraint curConstraint;
        switch (operator) {
            case EQUALS:
                // TRUE AND TRUE || FALSE AND FALSE
                curConstraint = leftTrue.intersects(rightTrue).union(leftFalse.intersects(rightFalse),
                        IotDBConstant.createFloatArithmeticTolerance());
                break;
            case NOT_EQUALS:
                // TRUE AND FALSE || FALSE AND TRUE
                curConstraint = leftTrue.intersects(rightFalse).union(leftFalse.intersects(rightTrue),
                        IotDBConstant.createFloatArithmeticTolerance());
                break;
            default:
                log.error("不支持Boolean值进行非等值比较");
                throw new IgnoreMeException();
        }
        // 生成单个约束条件
        constraintStack.add(ConstraintValueGenerator.genConstraint(ConstraintValueGenerator.genBaseTimeSeries(),
                curConstraint));
    }

    @Override
    public void visit(IotDBCastOperation op) {
        visit(op.getExpr());
        ConstraintValue exprValue = constraintStack.pop();
        // 其他类型强转均满足 不损失精度的转换, 故TSQS中强转不会对Y轴增加额外的约束条件
        if (exprValue.isTimeSeries() && op.getCastType().equalsIgnoreCase(IotDBCastOperation.CastType.BOOLEAN.getType())) {
            // 列强转BOOL值 -> 约束列不为0值(IotDB 非0值转为TRUE)
            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint(exprValue.getTimeSeriesName());
            timeSeriesConstraint.addNotEqualValue(BigDecimal.ZERO);
            constraintStack.add(ConstraintValueGenerator.genConstraint(exprValue.getTimeSeriesValue(),
                    timeSeriesConstraint));
        } else constraintStack.add(exprValue);
    }

    @Override
    public void visit(IotDBUnaryPrefixOperation op) {
        visit(op.getExpression());
        ConstraintValue constraintValue = constraintStack.pop();
        // 约束条件取反
        if (op.getOp() == IotDBUnaryPrefixOperation.IotDBUnaryPrefixOperator.MINUS)
            constraintStack.add(constraintValue.negate());
        else
            constraintStack.add(constraintValue);
    }

    @Override
    public void visit(IotDBUnaryNotPrefixOperation op) {
        // Unary NOT Prefix Operation
        visit(op.getExpression());
        ConstraintValue constraintValue = constraintStack.pop();
        if (constraintValue.isTimeSeries()) {
            // 列表达式系数被减法清除 => NOT 常量 => TRUE
            ConstraintValue.TimeSeriesValue timeSeriesValue = (ConstraintValue.TimeSeriesValue) constraintValue;
            if (timeSeriesValue.getFactor().getBigDecimalConstant().compareTo(BigDecimal.ZERO) == 0) {
                if (op.getExpression().getExpectedValue().castAs(IotDBCastOperation.CastType.BIGDECIMAL)
                        .getBigDecimalValue().compareTo(timeSeriesValue.getIntercept().getBigDecimalConstant()) == 0)
                    constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
                else constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
                return;
            }

            // 列预期值, (NOT timeSeries) 即将其预期值取TRUE
            BigDecimal expectedValue = op.getExpression().getExpectedValue().castAs(
                    IotDBCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
            // 消除列因子
            expectedValue = timeSeriesValue.eliminateFactor(expectedValue);
            BigDecimal lessValue = expectedValue;
            BigDecimal greaterValue = expectedValue;
            // 浮点运算存在精度损失
            if (BigDecimalUtil.isDouble(expectedValue)) {
                greaterValue = expectedValue.subtract(
                        BigDecimal.valueOf(Math.pow(10, -IotDBConstant.IotDBDoubleConstant.scale)));
                lessValue = expectedValue.add(
                        BigDecimal.valueOf(Math.pow(10, -IotDBConstant.IotDBDoubleConstant.scale)));
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
    public void visit(IotDBBinaryArithmeticOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
        ConstraintValue right = constraintStack.pop();
        ConstraintValue left = constraintStack.pop();

        IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator operator = op.getOp();
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
            if (operator == IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.DIVIDE) {
                timeSeriesValue.transToReciprocalVal(constantValue);
                constraintStack.add(timeSeriesValue);
                return;
            }
            // 针对列的减法 => 列约束条件取反, 变为加法
            if (operator == IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.SUBTRACT) {
                timeSeriesValue = timeSeriesValue.negate();
                operator = IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.PLUS;
            }
        } else if (left.isTimeSeries() && right.isConstant()) {
            timeSeriesValue = (ConstraintValue.TimeSeriesValue) left;
            constantValue = (ConstraintValue.ConstantValue) right;
        } else {
            log.error("约束op约束, left:{} right:{}", left, right);
            throw new IgnoreMeException();
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
    public void visit(IotDBOrderByTerm op) {
        // TODO
    }

    @Override
    public void visit(IotDBUnaryPostfixOperation op) {
        visit(op.getExpression());
        ConstraintValue exprValue = constraintStack.pop();

        if (exprValue.isConstant() && !op.isNegated())
            constraintStack.add(ConstraintValueGenerator.genFalseConstraint());
        else if (exprValue.isConstant() && op.isNegated())
            constraintStack.add(ConstraintValueGenerator.genTrueConstraint());
        else if (exprValue.isTimeSeries() && !op.isNegated()) {
            // timeSeries is null -> timestamp list
            constraintStack.add(ConstraintValueGenerator.genConstraint(
                    ConstraintValueGenerator.genTimeSeries(IotDBValueStateConstant.TIME_FIELD.getValue()),
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
    public void visit(IotDBInOperation op) {
        if (op.getListElements().isEmpty()) throw new AssertionError();

        // 将IN转换为OR表达式组合
        IotDBBinaryComparisonOperation.BinaryComparisonOperator comparisonOperator;
        IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator logicalOperator;
        if (!op.isTrue()) {
            comparisonOperator = IotDBBinaryComparisonOperation.BinaryComparisonOperator.NOT_EQUALS;
            logicalOperator = IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND;
        } else {
            comparisonOperator = IotDBBinaryComparisonOperation.BinaryComparisonOperator.EQUALS;
            logicalOperator = IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.OR;
        }

        int index = 0;
        IotDBExpression combinationExpr = new IotDBBinaryComparisonOperation(op.getExpr(),
                op.getListElements().get(index++), comparisonOperator);

        for (; index < op.getListElements().size(); index++) {
            IotDBBinaryComparisonOperation right = new IotDBBinaryComparisonOperation(op.getExpr(),
                    op.getListElements().get(index), comparisonOperator);
            combinationExpr = new IotDBBinaryLogicalOperation(combinationExpr, right, logicalOperator);
        }
        visit(combinationExpr);
    }

    @Override
    public void visit(IotDBBetweenOperation op) {
        // 将BETWEEN转换为LESS_EQUALS
        IotDBBinaryComparisonOperation left = new IotDBBinaryComparisonOperation(op.getLeft(), op.getExpr(),
                IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        IotDBBinaryComparisonOperation right = new IotDBBinaryComparisonOperation(op.getExpr(), op.getRight(),
                IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        visit(new IotDBBinaryLogicalOperation(left, right,
                IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND));
    }
}
