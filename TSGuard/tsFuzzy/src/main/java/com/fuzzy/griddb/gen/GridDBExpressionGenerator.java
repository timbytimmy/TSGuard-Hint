package com.fuzzy.griddb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema;
import com.fuzzy.griddb.GridDBSchema.GridDBColumn;
import com.fuzzy.griddb.GridDBSchema.GridDBDataType;
import com.fuzzy.griddb.ast.*;
import com.fuzzy.griddb.ast.GridDBBinaryArithmeticOperation.GridDBBinaryArithmeticOperator;
import com.fuzzy.griddb.ast.GridDBBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.griddb.ast.GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator;
import com.fuzzy.griddb.ast.GridDBBinaryOperation.GridDBBinaryOperator;
import com.fuzzy.griddb.ast.GridDBComputableFunction.GridDBFunction;
import com.fuzzy.griddb.ast.GridDBConstant.GridDBDoubleConstant;
import com.fuzzy.griddb.ast.GridDBOrderByTerm.GridDBOrder;
import com.fuzzy.griddb.ast.GridDBUnaryNotPrefixOperation.GridDBUnaryNotPrefixOperator;
import com.fuzzy.griddb.ast.GridDBUnaryPrefixOperation.GridDBUnaryPrefixOperator;
import com.fuzzy.griddb.feedback.GridDBQuerySynthesisFeedbackManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class GridDBExpressionGenerator extends UntypedExpressionGenerator<GridDBExpression, GridDBColumn> {

    private final GridDBGlobalState state;
    private GridDBSchema.GridDBRowValue rowVal;

    public GridDBExpressionGenerator(GridDBGlobalState state) {
        this.state = state;
    }

    public GridDBExpressionGenerator setRowVal(GridDBSchema.GridDBRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION,
        UNARY_NOT_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION,
        BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON_OPERATION, IN_OPERATION,
        BINARY_OPERATION, BINARY_ARITHMETIC_OPERATION,
        BETWEEN_OPERATOR, CAST_OPERATOR;
    }

    @Override
    public GridDBExpression generateExpression(int depth) {
        if (depth >= (state.getOptions().useSyntaxSequence() ?
                GridDBQuerySynthesisFeedbackManager.expressionDepth.get() : state.getOptions().getMaxExpressionDepth())) {
            return generateLeafNode();
        }

        Actions actions = Randomly.fromOptions(Actions.values());
        GridDBExpression expression;
        switch (actions) {
            case COLUMN:
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
            case UNARY_NOT_PREFIX_OPERATION:
                GridDBExpression subExpression = generateExpression(depth + 1);
                expression = new GridDBUnaryNotPrefixOperation(
                        subExpression, GridDBUnaryNotPrefixOperator.getRandom(subExpression));
                break;
            case UNARY_PREFIX_OPERATION:
                expression = new GridDBUnaryPrefixOperation(generateExpression(depth + 1),
                        GridDBUnaryPrefixOperator.getRandom());
                break;
            case CAST_OPERATOR:
                GridDBExpression columnExpr = generateColumn();
                expression = new GridDBCastOperation(columnExpr,
                        GridDBCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
                break;
            case UNARY_POSTFIX:
                expression = new GridDBUnaryPostfixOperation(generateExpression(depth + 1),
                        Randomly.fromOptions(GridDBUnaryPostfixOperation.UnaryPostfixOperator.values()),
                        Randomly.getBoolean());
                break;
            case COMPUTABLE_FUNCTION:
                expression = getComputableFunction(depth + 1);
                break;
            case BINARY_LOGICAL_OPERATOR:
                // 非Boolean值操作符不允许单独出现至AND等后面
                if (depth >= (state.getOptions().useSyntaxSequence() ?
                        GridDBQuerySynthesisFeedbackManager.expressionDepth.get() :
                        state.getOptions().getMaxExpressionDepth()) - 1)
                    expression = generateExpression(depth);
                else expression = new GridDBBinaryLogicalOperation(
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        GridDBBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new GridDBBinaryComparisonOperation(
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        BinaryComparisonOperator.getRandom());
                break;
            case IN_OPERATION:
                GridDBExpression expr = generateExpression(depth + 1);
                List<GridDBExpression> rightList = new ArrayList<>();
                for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                    rightList.add(generateExpression(depth + 1));
                expression = new GridDBInOperation(expr, rightList, Randomly.getBoolean());
                break;
            case BINARY_OPERATION:
                // TODO
                expression = generateExpression(depth);
//                expression = new GridDBBinaryOperation(
//                        generateExpression(depth + 1),
//                        generateExpression(depth + 1),
//                        GridDBBinaryOperator.getRandom());
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new GridDBBinaryArithmeticOperation(
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        GridDBBinaryArithmeticOperator.getRandom());
                break;
            case BETWEEN_OPERATOR:
                expression = new GridDBBetweenOperation(generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        generateExpression(depth + 1), false);
                break;
            default:
                throw new AssertionError();
        }
        // 校验语法正确性
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    @Override
    protected GridDBExpression generateExpression(Object parentActions, int depth) {
        try {
            return generateExpression(depth);
        } catch (ReGenerateExpressionException e) {
            return generateExpression(depth);
        }
    }

    @Override
    public GridDBExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        if (ObjectUtils.isEmpty(fatherActions)) return generateLeafNode();
        Actions father = Actions.valueOf(fatherActions);

        try {
            GridDBExpression expression;
            switch (father) {
                case COLUMN:
                    // GridDB 不支持 cross join, 故 使用AND操作替代, AND 操作不支持COLUMN、LITERAL单列出现, 故parentActions不应为null
                    expression = generateColumn();
                    break;
                case LITERAL:
                    expression = generateConstant();
                    break;
                case UNARY_NOT_PREFIX_OPERATION:
                    GridDBExpression subExpression = generateExpressionForSyntaxValidity(childActions, null);
                    expression = new GridDBUnaryNotPrefixOperation(
                            subExpression, GridDBUnaryNotPrefixOperator.getRandom(subExpression));
                    break;
                case UNARY_PREFIX_OPERATION:
                    GridDBExpression subExpr = generateExpressionForSyntaxValidity(childActions, null);
                    GridDBUnaryPrefixOperator random = GridDBUnaryPrefixOperator.getRandom();
                    expression = new GridDBUnaryPrefixOperation(subExpr, random);
                    break;
                case CAST_OPERATOR:
                    GridDBExpression columnExpr = generateColumn();
                    expression = new GridDBCastOperation(columnExpr,
                            GridDBCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
                    break;
                case UNARY_POSTFIX:
                    expression = new GridDBUnaryPostfixOperation(generateExpressionForSyntaxValidity(childActions, null),
                            Randomly.fromOptions(GridDBUnaryPostfixOperation.UnaryPostfixOperator.values()),
                            Randomly.getBoolean());
                    break;
                case COMPUTABLE_FUNCTION:
                    expression = generateExpressionForSyntaxValidity(fatherActions, childActions);
                    break;
                case BINARY_LOGICAL_OPERATOR:
                    expression = new GridDBBinaryLogicalOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            GridDBBinaryLogicalOperator.getRandom());
                    break;
                case BINARY_COMPARISON_OPERATION:
                    expression = new GridDBBinaryComparisonOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            BinaryComparisonOperator.getRandom());
                    break;
                case IN_OPERATION:
                    GridDBExpression expr = generateExpressionForSyntaxValidity(childActions, null);
                    List<GridDBExpression> rightList = new ArrayList<>();
                    for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                        rightList.add(generateLeafNode());
                    expression = new GridDBInOperation(expr, rightList, Randomly.getBoolean());
                    break;
                case BINARY_OPERATION:
                    expression = new GridDBBinaryOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            GridDBBinaryOperator.getRandom());
                    break;
                case BINARY_ARITHMETIC_OPERATION:
                    expression = new GridDBBinaryArithmeticOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            GridDBBinaryArithmeticOperator.getRandom());
                    break;
                case BETWEEN_OPERATOR:
                    expression = new GridDBBetweenOperation(generateExpressionForSyntaxValidity(childActions, null),
                            generateLeafNode(), generateLeafNode(), false);
                    break;
                default:
                    throw new AssertionError();
            }
            return expression;
        } catch (ReGenerateExpressionException e) {
            return generateExpressionForSyntaxValidity(fatherActions, childActions);
        }
    }

    private GridDBExpression ignoreThisExpr(int depth) {
        return generateExpression(depth);
    }

    private GridDBExpression getComputableFunction(int depth) {
        GridDBFunction func = GridDBFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) nrArgs += Randomly.smallNumber();
        GridDBExpression[] args = new GridDBExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            // TODO
//            args[i] = generateExpression(depth + 1);
            args[i] = generateConstant();
        }
        return new GridDBComputableFunction(func, args);
    }

    @Override
    public GridDBExpression generateConstant() {
        GridDBDataType[] values;
        if (state.usesPQS()) {
            values = GridDBDataType.valuesPQS();
        } else if (state.usesTSAF()) {
            values = GridDBDataType.valuesTSAF();
        } else {
            values = GridDBDataType.values();
        }
        switch (Randomly.fromOptions(values)) {
            case INTEGER:
                return GridDBConstant.createInt32Constant(state.getRandomly().getInteger());
            case LONG:
                return GridDBConstant.createInt64Constant(state.getRandomly().getInteger());
            case STRING:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return GridDBConstant.createStringConstant(string);
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
                        / state.getRandomly().getInteger()).setScale(
                        GridDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return GridDBDoubleConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    public GridDBExpression generateConstantForGridDBDataType(GridDBSchema.GridDBDataType gridDBDataType) {
        switch (gridDBDataType) {
            case INTEGER:
                return GridDBConstant.createInt32Constant(state.getRandomly().getInteger());
            case LONG:
                return GridDBConstant.createInt64Constant(state.getRandomly().getInteger());
            case BOOL:
                return GridDBConstant.createBoolean(Randomly.getBoolean());
            case STRING:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return GridDBConstant.createStringConstant(string);
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
                        / state.getRandomly().getInteger()).setScale(
                        GridDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return GridDBConstant.GridDBDoubleConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected GridDBExpression generateColumn() {
        GridDBColumn c = Randomly.fromList(columns);
        GridDBConstant val;
        if (rowVal == null) {
            // TSQS生成表达式时, rowVal默认值为1, 列存在因子时能够进行区分
            String databaseName = state.getDatabaseName();
            SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                    .getSamplingFrequencyFromCollection(databaseName, c.getTable().getName());
            BigDecimal bigDecimal = EquationsManager.getInstance().getEquationsFromTimeSeries(databaseName,
                            c.getTable().getName(), c.getName())
                    .genValueByTimestamp(samplingFrequency, state.getOptions().getStartTimestampOfTSData());
            val = c.getType().isInt() ? GridDBConstant.createInt64Constant(bigDecimal.longValue()) :
                    c.getType().isNumeric() ? GridDBConstant.createBigDecimalConstant(bigDecimal)
                            : GridDBConstant.createStringConstant(bigDecimal.toPlainString());
        } else val = rowVal.getValues().get(c);
        return GridDBColumnReference.create(c, val);
    }

    @Override
    public GridDBExpression negatePredicate(GridDBExpression predicate) {
        return null;
//        return new GridDBUnaryPrefixOperation(predicate, GridDBUnaryNotPrefixOperator.NOT);
    }

    @Override
    public GridDBExpression isNull(GridDBExpression expr) {
        return null;
//        return new GridDBUnaryPostfixOperation(expr, GridDBUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<GridDBExpression> generateOrderBys() {
        // order by columns
        List<GridDBColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<GridDBExpression> expressions = columnsForOrderBy.stream().map(column -> {
            GridDBConstant val = null;
            return new GridDBColumnReference(column, val);
        }).collect(Collectors.toList());
        List<GridDBExpression> newOrderBys = new ArrayList<>();
        for (GridDBExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                GridDBOrderByTerm newExpr = new GridDBOrderByTerm(expr, GridDBOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
