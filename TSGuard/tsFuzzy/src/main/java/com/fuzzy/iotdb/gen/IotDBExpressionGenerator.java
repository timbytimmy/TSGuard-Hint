package com.fuzzy.iotdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema.IotDBColumn;
import com.fuzzy.iotdb.IotDBSchema.IotDBDataType;
import com.fuzzy.iotdb.IotDBSchema.IotDBRowValue;
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.ast.IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator;
import com.fuzzy.iotdb.ast.IotDBBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.iotdb.ast.IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator;
import com.fuzzy.iotdb.ast.IotDBOrderByTerm.IotDBOrder;
import com.fuzzy.iotdb.ast.IotDBUnaryPrefixOperation.IotDBUnaryPrefixOperator;
import com.fuzzy.iotdb.feedback.IotDBQuerySynthesisFeedbackManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class IotDBExpressionGenerator extends UntypedExpressionGenerator<IotDBExpression, IotDBColumn> {

    public static final Set<String> pairingProhibited = new HashSet<>();
    private final IotDBGlobalState state;
    private IotDBRowValue rowVal;

    public IotDBExpressionGenerator(IotDBGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    public IotDBExpressionGenerator setRowVal(IotDBRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private void initGenerator() {
        // TODO 查询有效性问题（目前仅靠标记规避 -> 后续应考虑更有效的查询生成方法, 例如基于反馈机制）
        // TODO 重复生成查询问题 -> 记录已生成查询信息, 避免重复生成
        // TODO 表达式节点生成时, 是否应该筛选避免某些节点在Randomly.fromOptions(Actions.values())中出现?
        // TODO 分为两类: 传参仅支持BOOLEAN、传参仅支持数值
        // 301: The Type of three subExpression should be all Numeric or Text
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.IN_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.UNARY_POSTFIX));

        // 701: Invalid input expression data type. expression: true, actual data type: BOOLEAN, expected data type(s): [INT32, INT64, FLOAT, DOUBLE].
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.IN_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.UNARY_POSTFIX));

        //  701: The output type of the expression in WHERE clause should be BOOLEAN, actual data type: INT32.
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_ARITHMETIC_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.COLUMN));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.LITERAL));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.UNARY_PREFIX_OPERATION));
        // CAST(t1 as BOOLEAN)
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.CAST_OPERATOR));

        // 701: Invalid input expression data type. expression: 1, actual data type: INT64, expected data type(s): [BOOLEAN].
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_NOT_PREFIX_OPERATION, Actions.LITERAL));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_NOT_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION,
        BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON_OPERATION, IN_OPERATION, BINARY_OPERATION,
        BINARY_ARITHMETIC_OPERATION, BETWEEN_OPERATOR, CAST_OPERATOR;
    }

    @Override
    public IotDBExpression generateExpression(int depth) {

        return generateExpression(null, depth);
    }

    @Override
    protected IotDBExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= (state.getOptions().useSyntaxSequence() ?
                    IotDBQuerySynthesisFeedbackManager.expressionDepth.get() :
                    state.getOptions().getMaxExpressionDepth())) {
                return generateLeafNode();
            }
            Actions actions = Randomly.fromOptions(Actions.values());
            // 语法序列组合校验
            while (!checkExpressionValidity(parentActions, actions)) {
                actions = Randomly.fromOptions(Actions.values());
            }
            return generateSpecifiedExpression(actions, parentActions, depth);
        } catch (ReGenerateExpressionException e) {
            return generateExpression(parentActions, depth);
        }
    }

    protected IotDBExpression generateSpecifiedExpression(Actions actions, Object parentActions, int depth) {
        IotDBExpression expression;
        switch (actions) {
            case COLUMN:
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
            case CAST_OPERATOR:
                IotDBExpression columnExpr = generateColumn();
                expression = new IotDBCastOperation(columnExpr,
                        IotDBCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
                break;
            case UNARY_PREFIX_OPERATION:
                expression = new IotDBUnaryPrefixOperation(generateExpression(actions, depth + 1),
                        IotDBUnaryPrefixOperator.getRandom());
                break;
            case UNARY_NOT_PREFIX_OPERATION:
                IotDBExpression subExpr = generateExpression(actions, depth + 1);
                expression = new IotDBUnaryNotPrefixOperation(subExpr,
                        IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator.getRandom(subExpr));
                break;
            case UNARY_POSTFIX:
                expression = new IotDBUnaryPostfixOperation(generateExpression(actions, depth + 1),
                        Randomly.fromOptions(IotDBUnaryPostfixOperation.UnaryPostfixOperator.values()),
                        Randomly.getBoolean());
                break;
            case COMPUTABLE_FUNCTION:
                expression = ignoreThisExpr(parentActions, Actions.COMPUTABLE_FUNCTION, depth);
                break;
            case BINARY_LOGICAL_OPERATOR:
                if (depth >= (state.getOptions().useSyntaxSequence() ?
                        IotDBQuerySynthesisFeedbackManager.expressionDepth.get() :
                        state.getOptions().getMaxExpressionDepth()) - 1)
                    expression = generateExpression(parentActions, depth);
                else expression = new IotDBBinaryLogicalOperation(generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        IotDBBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new IotDBBinaryComparisonOperation(generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        BinaryComparisonOperator.getRandom());
                break;
            case IN_OPERATION:
                IotDBExpression expr = generateExpression(actions, depth + 1);
                List<IotDBExpression> rightList = new ArrayList<>();
                IotDBDataType dataType = expr.getExpectedValue().getType();
                for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                    // IN操作内部仅支持常量
                    rightList.add(generateConstant(dataType));
                }
                expression = new IotDBInOperation(expr, rightList, Randomly.getBoolean());
                break;
            case BINARY_OPERATION:
                // IotDB不支持二元位操作？
                expression = ignoreThisExpr(parentActions, Actions.BINARY_OPERATION, depth);
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new IotDBBinaryArithmeticOperation(generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        IotDBBinaryArithmeticOperator.getRandom());
                break;
            case BETWEEN_OPERATOR:
                expression = new IotDBBetweenOperation(generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1), false);
                break;
            default:
                throw new AssertionError();
        }
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    @Override
    public IotDBExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        if (ObjectUtils.isEmpty(fatherActions)) return generateLeafNode();
        IotDBExpressionGenerator.Actions father = IotDBExpressionGenerator.Actions.valueOf(fatherActions);

        try {
            switch (father) {
                case COLUMN:
                    return generateColumn();
                case LITERAL:
                    return generateConstant();
                case UNARY_NOT_PREFIX_OPERATION:
                    IotDBExpression subExpression = generateExpressionForSyntaxValidity(childActions, null);
                    return new IotDBUnaryNotPrefixOperation(
                            subExpression, IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator.getRandom(subExpression));
                case UNARY_PREFIX_OPERATION:
                    IotDBExpression subExpr = generateExpressionForSyntaxValidity(childActions, null);
                    IotDBUnaryPrefixOperator random = IotDBUnaryPrefixOperator.getRandom();
                    return new IotDBUnaryPrefixOperation(subExpr, random);
                case CAST_OPERATOR:
                    IotDBExpression columnExpr = generateColumn();
                    return new IotDBCastOperation(columnExpr,
                            IotDBCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
                case UNARY_POSTFIX:
                    return new IotDBUnaryPostfixOperation(generateExpressionForSyntaxValidity(childActions, null),
                            Randomly.fromOptions(IotDBUnaryPostfixOperation.UnaryPostfixOperator.values()),
                            Randomly.getBoolean());
                case COMPUTABLE_FUNCTION:
                    return generateExpressionForSyntaxValidity(fatherActions, childActions);
                case BINARY_LOGICAL_OPERATOR:
                    return new IotDBBinaryLogicalOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.getRandom());
                case BINARY_COMPARISON_OPERATION:
                    return new IotDBBinaryComparisonOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            IotDBBinaryComparisonOperation.BinaryComparisonOperator.getRandom());
                case IN_OPERATION:
                    IotDBExpression expr = generateExpressionForSyntaxValidity(childActions, null);
                    List<IotDBExpression> rightList = new ArrayList<>();
                    for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                        rightList.add(generateLeafNode());
                    return new IotDBInOperation(expr, rightList, Randomly.getBoolean());
                case BINARY_OPERATION:
                    return generateExpressionForSyntaxValidity(fatherActions, childActions);
                case BINARY_ARITHMETIC_OPERATION:
                    return new IotDBBinaryArithmeticOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.getRandom());
                case BETWEEN_OPERATOR:
                    IotDBExpression subExprForBetween = generateExpressionForSyntaxValidity(childActions, null);
                    return new IotDBBetweenOperation(subExprForBetween,
                            generateConstantForIotDBDataType(subExprForBetween.getExpectedValue().getType()),
                            generateConstantForIotDBDataType(subExprForBetween.getExpectedValue().getType()), false);
                default:
                    throw new AssertionError();
            }
        } catch (ReGenerateExpressionException e) {
            return generateExpressionForSyntaxValidity(fatherActions, childActions);
        }
    }

    private IotDBExpression ignoreThisExpr(Object parentActions, IotDBExpressionGenerator.Actions action, int depth) {
        return generateExpression(parentActions, depth);
    }

    private boolean checkExpressionValidity(final Object parentActions,
                                            final IotDBExpressionGenerator.Actions childActions) {
        if (parentActions == null) return true;
        else if (pairingProhibited.contains(
                genHashKeyWithPairActions(IotDBExpressionGenerator.Actions.valueOf(parentActions.toString()),
                        childActions))) {
            return false;
        }
        return true;
    }

    private String genHashKeyWithPairActions(final IotDBExpressionGenerator.Actions parentNode,
                                             final IotDBExpressionGenerator.Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    private IotDBExpression getComputableFunction(int depth) {
        return null;
    }

    @Override
    public IotDBExpression generateConstant() {
        IotDBDataType[] values;
        if (state.usesPQS()) {
            values = IotDBDataType.valuesPQS();
        } else if (state.usesTSAF() || state.usesHINT()) {
            // TODO
            values = new IotDBDataType[]{IotDBDataType.INT32};
        } else {
            values = IotDBDataType.values();
        }
        switch (Randomly.fromOptions(values)) {
            case INT32:
                return IotDBConstant.createInt32Constant((int) state.getRandomly().getInteger(-100000, 100000));
            case INT64:
                return IotDBConstant.createInt64Constant(state.getRandomly().getLong(Integer.MIN_VALUE, Integer.MAX_VALUE));
            case TEXT:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return IotDBConstant.createStringConstant(string);
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf(state.getRandomly().getInfiniteDouble()).setScale(
                        IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return IotDBConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    public IotDBExpression generateConstant(IotDBDataType dataType) {
        // TODO
        if (state.usesTSAF() || state.usesHINT()){
            dataType = IotDBDataType.INT32;
        }
        switch (dataType) {
            case INT32:
                return IotDBConstant.createInt32Constant((int) state.getRandomly().getInteger());
            case INT64:
                return IotDBConstant.createInt64Constant(state.getRandomly().getLong());
            case TEXT:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return IotDBConstant.createStringConstant(string);
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf(state.getRandomly().getInfiniteDouble()).setScale(
                        IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return IotDBConstant.createDoubleConstant(val);
            case BOOLEAN:
                return IotDBConstant.createBoolean(Randomly.getBoolean());
            case NULL:
                return IotDBConstant.createNullConstant();
            default:
                throw new AssertionError();
        }
    }

    public IotDBExpression generateConstantForIotDBDataType(IotDBDataType IotDBDataType) {
        switch (IotDBDataType) {
            case INT32:
                return IotDBConstant.createInt32Constant(state.getRandomly().getInteger());
            case INT64:
                return IotDBConstant.createInt64Constant(state.getRandomly().getLong());
            case BOOLEAN:
                return IotDBConstant.createBoolean(Randomly.getBoolean());
            case TEXT:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return IotDBConstant.createStringConstant(string);
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf(state.getRandomly().getInfiniteDouble()).setScale(
                        IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return IotDBConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected IotDBExpression generateColumn() {
        IotDBColumn c = Randomly.fromList(columns);
        IotDBConstant val;
        if (rowVal == null) {
            // TSQS生成表达式时, rowVal默认值为1, 列存在因子时能够进行区分
            String databaseName = state.getDatabaseName();
            SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                    .getSamplingFrequencyFromCollection(databaseName, c.getTable().getName());
            BigDecimal bigDecimal = EquationsManager.getInstance().getEquationsFromTimeSeries(databaseName,
                            c.getTable().getName(), c.getName())
                    .genValueByTimestamp(samplingFrequency, state.getOptions().getStartTimestampOfTSData());
            val = c.getType().isInt() ? IotDBConstant.createInt64Constant(bigDecimal.longValue()) :
                    c.getType().isNumeric() ? IotDBConstant.createBigDecimalConstant(bigDecimal)
                            : IotDBConstant.createStringConstant(bigDecimal.toPlainString());
        } else val = rowVal.getValues().get(c);
        return IotDBColumnReference.create(c, val);
    }

    @Override
    public IotDBExpression negatePredicate(IotDBExpression predicate) {
        return null;
//        return new IotDBUnaryPrefixOperation(predicate, IotDBUnaryPrefixOperator.NOT);
    }

    @Override
    public IotDBExpression isNull(IotDBExpression expr) {
        return null;
//        return new IotDBUnaryPostfixOperation(expr, IotDBUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<IotDBExpression> generateOrderBys() {
        // order by columns
        List<IotDBColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<IotDBExpression> expressions = columnsForOrderBy.stream().map(column -> {
            IotDBConstant val;
            if (rowVal == null) {
                val = null;
            } else {
                val = rowVal.getValues().get(column);
            }
            return new IotDBColumnReference(column, val);
        }).collect(Collectors.toList());
        List<IotDBExpression> newOrderBys = new ArrayList<>();
        for (IotDBExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                IotDBOrderByTerm newExpr = new IotDBOrderByTerm(expr, IotDBOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
