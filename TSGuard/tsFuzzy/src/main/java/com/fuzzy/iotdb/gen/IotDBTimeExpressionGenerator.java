package com.fuzzy.iotdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema.IotDBColumn;
import com.fuzzy.iotdb.ast.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class IotDBTimeExpressionGenerator extends UntypedExpressionGenerator<IotDBExpression, IotDBColumn> {

    // 父节点后不允许跟子节点
    private static final int maxDepth = 2;
    private static final Set<String> pairingProhibited = new HashSet<>();
    private final IotDBGlobalState state;
    private boolean hasTimeColumn = false;

    public IotDBTimeExpressionGenerator(IotDBGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    private void initGenerator() {
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.COLUMN));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.LITERAL));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_ARITHMETIC_OPERATION));

        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_COMPARISON_OPERATION));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BINARY_LOGICAL_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.BETWEEN_OPERATOR));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BETWEEN_OPERATOR, Actions.IN_OPERATION));

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
    protected IotDBExpression generateExpression(Object parentActions, int depth) {
        try {
            if (depth >= maxDepth) {
                if (hasTimeColumn) return generateLeafNode();
                hasTimeColumn = true;
                return generateColumn();
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
            case BINARY_LOGICAL_OPERATOR:
                // 非Boolean值操作符不允许单独出现至AND等后面
                if (depth >= maxDepth - 1) expression = generateExpression(parentActions, depth);
                else expression = new IotDBBinaryLogicalOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                // TODO 不生成类似 ) <= (time) 查询, 该查询存在bug, 已报告
                expression = new IotDBBinaryComparisonOperation(
//                        generateExpression(actions, depth + 1),
//                        generateExpression(actions, depth + 1),
                        generateColumn(),
                        generateConstant(),
                        IotDBBinaryComparisonOperation.BinaryComparisonOperator.getRandom());
                break;
            case BETWEEN_OPERATOR:
                expression = new IotDBBetweenOperation(generateExpression(actions, depth + 1),
                        generateConstant(),
                        generateConstant(), false);
                break;
            case IN_OPERATION:
                IotDBExpression expr = generateExpression(actions, depth + 1);
                List<IotDBExpression> rightList = new ArrayList<>();
                for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                    rightList.add(generateConstant());
                expression = new IotDBInOperation(expr, rightList, Randomly.getBoolean());
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new IotDBBinaryArithmeticOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.getRandomOperatorForTimestamp());
                break;
            default:
                throw new AssertionError();
        }
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    @Override
    public IotDBExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        return null;
    }

    private IotDBExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
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
    public IotDBExpression generateConstant() {
        return IotDBConstant.createInt64Constant(state.getRandomTimestamp());
    }

    @Override
    protected IotDBExpression generateExpression(int depth) {
        return null;
    }

    @Override
    public IotDBExpression generateColumn() {
        IotDBColumn c = Randomly.fromList(columns);
        return IotDBColumnReference.create(c, IotDBConstant.createInt64Constant(
                state.getOptions().getStartTimestampOfTSData()));
    }

    @Override
    public IotDBExpression negatePredicate(IotDBExpression predicate) {
        return null;
    }

    @Override
    public IotDBExpression isNull(IotDBExpression expr) {
        return null;
    }

    @Override
    public List<IotDBExpression> generateOrderBys() {
        // order by columns
        List<IotDBColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<IotDBExpression> expressions = columnsForOrderBy.stream().map(column -> {
            return new IotDBColumnReference(column, null);
        }).collect(Collectors.toList());
        List<IotDBExpression> newOrderBys = new ArrayList<>();
        for (IotDBExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                IotDBOrderByTerm newExpr = new IotDBOrderByTerm(expr, IotDBOrderByTerm.IotDBOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
