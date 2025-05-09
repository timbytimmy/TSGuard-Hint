package com.fuzzy.TDengine.gen;


import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema.TDengineColumn;
import com.fuzzy.TDengine.ast.*;
import com.fuzzy.TDengine.ast.TDengineOrderByTerm.TDengineOrder;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class TDengineTimeExpressionGenerator extends UntypedExpressionGenerator<TDengineExpression, TDengineColumn> {

    // 父节点后不允许跟子节点
    private static final int maxDepth = 2;
    private static final Set<String> pairingProhibited = new HashSet<>();
    private final TDengineGlobalState state;
    private boolean hasTimeColumn = false;

    public TDengineTimeExpressionGenerator(TDengineGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    private void initGenerator() {
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.COLUMN));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.LITERAL));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_ARITHMETIC_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_COMPARISON_OPERATION, Actions.IN_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.IN_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.IN_OPERATION, Actions.IN_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_ARITHMETIC_OPERATION, Actions.IN_OPERATION));
    }

    private String genHashKeyWithPairActions(final Actions parentNode, final Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    private enum Actions {
        COLUMN(false), LITERAL(false), BINARY_ARITHMETIC_OPERATION(false),
        BINARY_LOGICAL_OPERATOR(true), BINARY_COMPARISON_OPERATION(true),
        IN_OPERATION(true), BETWEEN_OPERATOR(true);

        private boolean resultIsLogic;

        Actions(boolean resultIsLogic) {
            this.resultIsLogic = resultIsLogic;
        }

        public boolean isResultIsLogic() {
            return resultIsLogic;
        }
    }

    @Override
    protected TDengineExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= maxDepth) {
                if (hasTimeColumn) return generateLeafNode();
                hasTimeColumn = true;
                return generateColumn();
            }
            Actions actions;
            do {
                actions = Randomly.fromOptions(Actions.values());
            } while (!checkExpressionValidity(parentActions, actions));
            TDengineExpression expression;
            switch (actions) {
                case COLUMN:
                    expression = generateColumn();
                    break;
                case LITERAL:
                    expression = generateConstant();
                    break;
                case BINARY_LOGICAL_OPERATOR:
                    // 非Boolean值操作符不允许单独出现至AND等后面
                    if (depth >= maxDepth - 1) expression = generateExpression(parentActions, depth);
                        // TODO TDengine Time表达式接OR存在逻辑错误, 暂时移除
                    else expression = new TDengineBinaryLogicalOperation(
                            generateExpression(actions, depth + 1),
                            generateExpression(actions, depth + 1),
                            TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);
                    break;
                case BINARY_COMPARISON_OPERATION:
                    expression = new TDengineBinaryComparisonOperation(
                            generateExpression(actions, depth + 1),
                            generateExpression(actions, depth + 1),
                            TDengineBinaryComparisonOperation.BinaryComparisonOperator.getRandom());
                    break;
                case BETWEEN_OPERATOR:
                    expression = new TDengineBetweenOperation(generateExpression(actions, depth + 1),
                            generateExpression(actions, depth + 1),
                            generateExpression(actions, depth + 1), false);
                    break;
                case IN_OPERATION:
                    TDengineExpression expr = generateExpression(actions, depth + 1);
                    List<TDengineExpression> rightList = new ArrayList<>();
                    for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                        rightList.add(generateConstant());
                    expression = new TDengineInOperation(expr, rightList, Randomly.getBoolean());
                    break;
                case BINARY_ARITHMETIC_OPERATION:
                    expression = new TDengineBinaryArithmeticOperation(
                            generateExpression(actions, depth + 1),
                            generateExpression(actions, depth + 1),
                            TDengineBinaryArithmeticOperation.TDengineBinaryArithmeticOperator.getRandomOperatorForTimestamp());
                    break;
                default:
                    throw new AssertionError();
            }
            if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
            return expression;
        } catch (ReGenerateExpressionException e) {
            return generateExpression(parentActions, depth);
        }
    }

    @Override
    public TDengineExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        return null;
    }

    private TDengineExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
        return generateExpression(parentActions, depth);
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
    public TDengineExpression generateConstant() {
        return TDengineConstant.createUInt64Constant(state.getRandomTimestamp());
    }

    @Override
    protected TDengineExpression generateExpression(int depth) {
        return null;
    }

    @Override
    protected TDengineExpression generateColumn() {
        TDengineColumn c = Randomly.fromList(columns);
        return TDengineColumnReference.create(c, TDengineConstant.createUInt64Constant(
                state.getOptions().getStartTimestampOfTSData()));
    }

    @Override
    public TDengineExpression negatePredicate(TDengineExpression predicate) {
        return null;
    }

    @Override
    public TDengineExpression isNull(TDengineExpression expr) {
        return null;
    }

    @Override
    public List<TDengineExpression> generateOrderBys() {
        // order by columns
        List<TDengineColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<TDengineExpression> expressions = columnsForOrderBy.stream().map(column -> {
            return new TDengineColumnReference(column, null);
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
