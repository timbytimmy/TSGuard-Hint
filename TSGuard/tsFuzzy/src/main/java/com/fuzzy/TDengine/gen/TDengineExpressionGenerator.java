package com.fuzzy.TDengine.gen;


import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema.TDengineColumn;
import com.fuzzy.TDengine.TDengineSchema.TDengineDataType;
import com.fuzzy.TDengine.TDengineSchema.TDengineRowValue;
import com.fuzzy.TDengine.ast.*;
import com.fuzzy.TDengine.ast.TDengineBinaryArithmeticOperation.TDengineBinaryArithmeticOperator;
import com.fuzzy.TDengine.ast.TDengineBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.TDengine.ast.TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator;
import com.fuzzy.TDengine.ast.TDengineBinaryOperation.TDengineBinaryOperator;
import com.fuzzy.TDengine.ast.TDengineConstant.TDengineDoubleConstant;
import com.fuzzy.TDengine.ast.TDengineOrderByTerm.TDengineOrder;
import com.fuzzy.TDengine.ast.TDengineUnaryNotPrefixOperation.TDengineUnaryNotPrefixOperator;
import com.fuzzy.TDengine.ast.TDengineUnaryPrefixOperation.TDengineUnaryPrefixOperator;
import com.fuzzy.TDengine.feedback.TDengineQuerySynthesisFeedbackManager;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
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
public class TDengineExpressionGenerator extends UntypedExpressionGenerator<TDengineExpression, TDengineColumn> {

    // 父节点后不允许跟子节点
    public static final Set<String> pairingProhibited = new HashSet<>();
    private final TDengineGlobalState state;
    private TDengineRowValue rowVal;

    public TDengineExpressionGenerator(TDengineGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    public TDengineExpressionGenerator setRowVal(TDengineRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private void initGenerator() {
        // 查询有效性（靠标记规避语法不符合的节点）
        // 经过严格配对实验，以下各种组合均属于语法错误
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_PREFIX_OPERATION, Actions.BINARY_COMPARISON_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_POSTFIX, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_POSTFIX, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_POSTFIX, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_POSTFIX, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_POSTFIX, Actions.IN_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.UNARY_POSTFIX, Actions.BETWEEN_OPERATOR));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.COLUMN));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.LITERAL));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.UNARY_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_ARITHMETIC_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.CAST_OPERATOR));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.IN_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_OPERATION, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_OPERATION, Actions.IN_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.IN_OPERATION));

        // BETWEEN 不支持BOOL
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.IN_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.UNARY_NOT_PREFIX_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.UNARY_POSTFIX));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.IN_OPERATION));
    }

    private String genHashKeyWithPairActions(final Actions parentNode, final Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    private enum Actions {
        COLUMN(false), LITERAL(false), UNARY_PREFIX_OPERATION(false),
        UNARY_NOT_PREFIX_OPERATION(true), UNARY_POSTFIX(true), COMPUTABLE_FUNCTION(false),
        BINARY_LOGICAL_OPERATOR(true), BINARY_COMPARISON_OPERATION(true), IN_OPERATION(true),
        BINARY_OPERATION(false), BINARY_ARITHMETIC_OPERATION(false),
        BETWEEN_OPERATOR(true), CAST_OPERATOR(false);

        private boolean resultIsLogic;

        Actions(boolean resultIsLogic) {
            this.resultIsLogic = resultIsLogic;
        }

        public boolean isResultIsLogic() {
            return resultIsLogic;
        }
    }

    @Override
    public TDengineExpression generateExpression(int depth) {
        return null;
    }

    @Override
    protected TDengineExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= (state.getOptions().useSyntaxSequence() ?
                    TDengineQuerySynthesisFeedbackManager.expressionDepth.get() :
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

    private TDengineExpression generateSpecifiedExpression(Actions actions, Object parentActions, int depth) {
        TDengineExpression expression;
        switch (actions) {
            case COLUMN:
                // TDengine 不支持 cross join, 故 使用AND操作替代, AND 操作不支持COLUMN、LITERAL单列出现, 故parentActions不应为null
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
            case UNARY_NOT_PREFIX_OPERATION:
                TDengineExpression subExpression = generateExpression(actions, depth + 1);
                expression = new TDengineUnaryNotPrefixOperation(
                        subExpression, TDengineUnaryNotPrefixOperator.getRandom(subExpression));
                break;
            case UNARY_PREFIX_OPERATION:
                expression = new TDengineUnaryPrefixOperation(generateExpression(actions, depth + 1),
                        TDengineUnaryPrefixOperator.getRandom());
                break;
            case CAST_OPERATOR:
                TDengineExpression columnExpr = generateColumn();
                expression = new TDengineCastOperation(columnExpr,
                        TDengineCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
                break;
            case UNARY_POSTFIX:
                expression = new TDengineUnaryPostfixOperation(generateExpression(actions, depth + 1),
                        Randomly.fromOptions(TDengineUnaryPostfixOperation.UnaryPostfixOperator.values()),
                        Randomly.getBoolean());
                break;
            case COMPUTABLE_FUNCTION:
                expression = getComputableFunction();
                break;
            case BINARY_LOGICAL_OPERATOR:
                // 非Boolean值操作符不允许单独出现至AND等后面
                if (depth >= (state.getOptions().useSyntaxSequence() ?
                        TDengineQuerySynthesisFeedbackManager.expressionDepth.get() :
                        state.getOptions().getMaxExpressionDepth()) - 1)
                    expression = generateExpression(parentActions, depth);
                else expression = new TDengineBinaryLogicalOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        TDengineBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new TDengineBinaryComparisonOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        BinaryComparisonOperator.getRandom());
                break;
            case IN_OPERATION:
                TDengineExpression expr = generateExpression(actions, depth + 1);
                List<TDengineExpression> rightList = new ArrayList<>();
                for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                    // TODO reported bug: IN 类型不一致处理逻辑存在问题, 改为查找类型一致
                    rightList.add(generateConstantForTDengineDataTypeForTSAF(expr.getExpectedValue().getType()));
                expression = new TDengineInOperation(expr, rightList, Randomly.getBoolean());
                break;
            case BINARY_OPERATION:
                expression = new TDengineBinaryOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        TDengineBinaryOperator.getRandom());
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new TDengineBinaryArithmeticOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        TDengineBinaryArithmeticOperator.getRandom());
                break;
            case BETWEEN_OPERATOR:
                expression = new TDengineBetweenOperation(generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1), false);
                break;
            default:
                throw new AssertionError();
        }
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    private boolean checkExpressionValidity(final Object parentActions, final Actions childActions) {
        // 非Boolean值操作符不允许单独出现至AND等后面
        if (parentActions == null && !childActions.isResultIsLogic()) return false;
        else if (parentActions == null) return true;
        else if (pairingProhibited.contains(genHashKeyWithPairActions(Actions.valueOf(parentActions.toString()),
                childActions)))
            return false;

        return true;
    }

    @Override
    public TDengineExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        if (ObjectUtils.isEmpty(fatherActions)) return generateLeafNode();
        Actions father = Actions.valueOf(fatherActions);

        try {
            switch (father) {
                case COLUMN:
                    // TDengine 不支持 cross join, 故 使用AND操作替代, AND 操作不支持COLUMN、LITERAL单列出现, 故parentActions不应为null
                    return generateColumn();
                case LITERAL:
                    return generateConstant();
                case UNARY_NOT_PREFIX_OPERATION:
                    TDengineExpression subExpression = generateExpressionForSyntaxValidity(childActions, null);
                    return new TDengineUnaryNotPrefixOperation(
                            subExpression, TDengineUnaryNotPrefixOperator.getRandom(subExpression));
                case UNARY_PREFIX_OPERATION:
                    TDengineExpression subExpr = generateExpressionForSyntaxValidity(childActions, null);
                    TDengineUnaryPrefixOperator random = TDengineUnaryPrefixOperator.getRandom();
                    return new TDengineUnaryPrefixOperation(subExpr, random);
                case CAST_OPERATOR:
                    TDengineExpression columnExpr = generateColumn();
                    return new TDengineCastOperation(columnExpr,
                            TDengineCastOperation.CastType.getRandom(columnExpr.getExpectedValue().getType()));
                case UNARY_POSTFIX:
                    return new TDengineUnaryPostfixOperation(generateExpressionForSyntaxValidity(childActions, null),
                            Randomly.fromOptions(TDengineUnaryPostfixOperation.UnaryPostfixOperator.values()),
                            Randomly.getBoolean());
                case COMPUTABLE_FUNCTION:
                    return generateExpressionForSyntaxValidity(fatherActions, childActions);
                case BINARY_LOGICAL_OPERATOR:
                    return new TDengineBinaryLogicalOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            TDengineBinaryLogicalOperator.getRandom());
                case BINARY_COMPARISON_OPERATION:
                    return new TDengineBinaryComparisonOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            BinaryComparisonOperator.getRandom());
                case IN_OPERATION:
                    TDengineExpression expr = generateExpressionForSyntaxValidity(childActions, null);
                    List<TDengineExpression> rightList = new ArrayList<>();
                    for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                        rightList.add(generateLeafNode());
                    return new TDengineInOperation(expr, rightList, Randomly.getBoolean());
                case BINARY_OPERATION:
                    return new TDengineBinaryOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            TDengineBinaryOperator.getRandom());
                case BINARY_ARITHMETIC_OPERATION:
                    return new TDengineBinaryArithmeticOperation(
                            generateExpressionForSyntaxValidity(childActions, null),
                            generateExpressionForSyntaxValidity(childActions, null),
                            TDengineBinaryArithmeticOperator.getRandom());
                case BETWEEN_OPERATOR:
                    return new TDengineBetweenOperation(generateExpressionForSyntaxValidity(childActions, null),
                            generateLeafNode(), generateLeafNode(), false);
                default:
                    throw new AssertionError();
            }
        } catch (ReGenerateExpressionException e) {
            return generateExpressionForSyntaxValidity(fatherActions, childActions);
        }
    }

    private TDengineExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
        return generateExpression(parentActions, depth);
    }

    private TDengineExpression getComputableFunction() {
        TDengineComputableFunction.TDengineFunction func = TDengineComputableFunction.TDengineFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) nrArgs += Randomly.smallNumber();
        TDengineExpression[] args = new TDengineExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = func.limitValueRange((TDengineConstant) generateConstant());
        }
        return new TDengineComputableFunction(func, args);
    }

    @Override
    public TDengineExpression generateConstant() {
        TDengineDataType[] values;
        if (state.usesPQS()) {
            values = TDengineDataType.valuesPQS();
        } else if (state.usesTSAF()) {
            values = TDengineDataType.valuesTSAF();
        } else {
            values = TDengineDataType.values();
        }
        switch (Randomly.fromOptions(values)) {
            case INT:
                return TDengineConstant.createInt32Constant(state.getRandomly().getInteger());
            case UINT:
                return TDengineConstant.createUInt32Constant(state.getRandomly().getUnsignedInteger());
            case BIGINT:
                // TODO 减少精度相关报错
                return TDengineConstant.createInt64Constant(state.getRandomly().getInteger());
            case UBIGINT:
                return TDengineConstant.createUInt64Constant(state.getRandomly().getUnsignedInteger());
            case BINARY:
            case VARCHAR:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return TDengineConstant.createStringConstant(string);
            case DOUBLE:
                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
                        / state.getRandomly().getInteger()).setScale(
                        TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return TDengineDoubleConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    public TDengineExpression generateConstantForTDengineDataType(TDengineDataType TDengineDataType) {
        switch (TDengineDataType) {
            case INT:
                return TDengineConstant.createInt32Constant(state.getRandomly().getInteger());
            case UINT:
                return TDengineConstant.createUInt32Constant(state.getRandomly().getUnsignedInteger());
            case BIGINT:
                return TDengineConstant.createInt64Constant(state.getRandomly().getInteger());
            case UBIGINT:
                return TDengineConstant.createUInt64Constant(state.getRandomly().getUnsignedInteger());
            case BOOL:
                return TDengineConstant.createBoolean(Randomly.getBoolean());
            case BINARY:
            case VARCHAR:
                String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
                return TDengineConstant.createStringConstant(string);
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
                        / state.getRandomly().getInteger()).setScale(
                        TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return TDengineDoubleConstant.createDoubleConstant(val);
            default:
                throw new AssertionError();
        }
    }

    public TDengineExpression generateConstantForTDengineDataTypeForTSAF(TDengineDataType tDengineDataType) {
        switch (tDengineDataType) {
            case BINARY:
            case VARCHAR:
            case INT:
                return TDengineConstant.createInt32Constant(state.getRandomly().getInteger());
            case UINT:
                return TDengineConstant.createUInt32Constant(state.getRandomly().getUnsignedInteger());
            case BIGINT:
                return TDengineConstant.createInt64Constant(state.getRandomly().getInteger());
            case UBIGINT:
                return TDengineConstant.createUInt64Constant(state.getRandomly().getUnsignedInteger());
            case BOOL:
                return TDengineConstant.createBoolean(Randomly.getBoolean());
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                double val = BigDecimal.valueOf((double) state.getRandomly().getInteger()
                        / state.getRandomly().getInteger()).setScale(
                        TDengineConstant.TDengineDoubleConstant.scale, RoundingMode.HALF_UP).doubleValue();
                return TDengineDoubleConstant.createDoubleConstant(val);
            case NULL:
                return TDengineConstant.createNullConstant();
            default:
                throw new AssertionError(String.format("%s", tDengineDataType));
        }
    }

    @Override
    protected TDengineExpression generateColumn() {
        TDengineColumn c = Randomly.fromList(columns);
        TDengineConstant val;
        if (rowVal == null) {
            // TSQS生成表达式时, rowVal默认值为1, 列存在因子时能够进行区分
            SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                    .getSamplingFrequencyFromCollection(state.getDatabaseName(), c.getTable().getName());
            BigDecimal bigDecimal = EquationsManager.getInstance().getEquationsFromTimeSeries(state.getDatabaseName(),
                            c.getTable().getName(), c.getName())
                    .genValueByTimestamp(samplingFrequency, state.getOptions().getStartTimestampOfTSData());
            val = c.getType().isInt() ? TDengineConstant.createInt64Constant(bigDecimal.longValue()) :
                    c.getType().isNumeric() ? TDengineConstant.createBigDecimalConstant(bigDecimal)
                            : TDengineConstant.createStringConstant(bigDecimal.toPlainString());
        } else val = rowVal.getValues().get(c);
        return TDengineColumnReference.create(c, val);
    }

    @Override
    public TDengineExpression negatePredicate(TDengineExpression predicate) {
        return null;
//        return new TDengineUnaryPrefixOperation(predicate, TDengineUnaryNotPrefixOperator.NOT);
    }

    @Override
    public TDengineExpression isNull(TDengineExpression expr) {
        return null;
//        return new TDengineUnaryPostfixOperation(expr, TDengineUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<TDengineExpression> generateOrderBys() {
        // order by columns
        List<TDengineColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<TDengineExpression> expressions = columnsForOrderBy.stream().map(column -> {
            TDengineConstant val;
            if (rowVal == null) {
                val = null;
            } else {
                val = rowVal.getValues().get(column);
            }
            return new TDengineColumnReference(column, val);
        }).collect(Collectors.toList());
        List<TDengineExpression> newOrderBys = new ArrayList<>();
        for (TDengineExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                TDengineOrderByTerm newExpr = new TDengineOrderByTerm(expr, TDengineOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
