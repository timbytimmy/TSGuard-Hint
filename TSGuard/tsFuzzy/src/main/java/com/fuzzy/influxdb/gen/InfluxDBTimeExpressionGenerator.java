package com.fuzzy.influxdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.ast.*;
import com.fuzzy.influxdb.ast.InfluxDBBinaryComparisonOperation.BinaryComparisonOperator;
import com.fuzzy.influxdb.ast.InfluxDBBinaryLogicalOperation.InfluxDBBinaryLogicalOperator;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class InfluxDBTimeExpressionGenerator extends UntypedExpressionGenerator<InfluxDBExpression, InfluxDBColumn> {

    // 父节点后不允许跟子节点
    private static final int maxDepth = 2;
    private static final Set<String> pairingProhibited = new HashSet<>();
    private final InfluxDBGlobalState state;
    private boolean hasTimeColumn = false;

    public InfluxDBTimeExpressionGenerator(InfluxDBGlobalState state) {
        this.state = state;
        if (state.getOptions().useSyntaxValidator()) initGenerator();
    }

    private void initGenerator() {
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.COLUMN));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.LITERAL));
        pairingProhibited.add(genHashKeyWithPairActions(Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_ARITHMETIC_OPERATION));
    }

    private String genHashKeyWithPairActions(final Actions parentNode, final Actions childNode) {
        return String.format("%s__%s", parentNode.name(), childNode.name());
    }

    private enum Actions {
        COLUMN(false), LITERAL(false),
        BINARY_LOGICAL_OPERATOR(true), BINARY_COMPARISON_OPERATION(true),
        BINARY_ARITHMETIC_OPERATION(false);

        private boolean resultIsLogic;

        Actions(boolean resultIsLogic) {
            this.resultIsLogic = resultIsLogic;
        }

        public boolean isResultIsLogic() {
            return resultIsLogic;
        }
    }

    @Override
    protected InfluxDBExpression generateExpression(Object parentActions, int depth) {
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

    protected InfluxDBExpression generateSpecifiedExpression(Actions actions, Object parentActions, int depth) {
        InfluxDBExpression expression;
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
                else expression = new InfluxDBBinaryLogicalOperation(
                        generateExpression(actions, depth + 1),
                        generateExpression(actions, depth + 1),
                        InfluxDBBinaryLogicalOperator.AND);
                break;
            case BINARY_COMPARISON_OPERATION:
                // InfluxDB不支持time和time两侧进行比较
                InfluxDBExpression constant = Randomly.getBoolean() ? generateConstant() :
                        new InfluxDBBinaryArithmeticOperation(
                                generateConstant(),
                                generateConstant(),
                                InfluxDBBinaryArithmeticOperation.InfluxDBBinaryArithmeticOperator
                                        .getRandomOperatorForTimestamp());
                expression = new InfluxDBBinaryComparisonOperation(
                        generateColumn(),
                        constant,
                        BinaryComparisonOperator.getRandomForTimestamp());
                break;
            case BINARY_ARITHMETIC_OPERATION:
                expression = new InfluxDBBinaryArithmeticOperation(
                        generateConstant(),
                        generateConstant(),
                        InfluxDBBinaryArithmeticOperation.InfluxDBBinaryArithmeticOperator.getRandomOperatorForTimestamp());
                break;
            default:
                throw new AssertionError();
        }
        if (state.getOptions().useSyntaxValidator()) expression.checkSyntax();
        return expression;
    }

    @Override
    public InfluxDBExpression generateExpressionForSyntaxValidity(String fatherActions, String childActions) {
        return null;
    }

    private InfluxDBExpression ignoreThisExpr(Object parentActions, Actions action, int depth) {
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

    //setting the time range of insert data
    @Override
    public InfluxDBExpression generateConstant() {
        long start = state.getOptions().getStartTimestampOfTSData();
        long end = start + 1000;
        long ts = state.getRandomly().getLong(start, end);
        return InfluxDBConstant.createUnsignedIntConstant(ts);
    }

    @Override
    protected InfluxDBExpression generateExpression(int depth) {
        return null;
    }

    @Override
    public InfluxDBExpression generateColumn() {
        InfluxDBColumn c = Randomly.fromList(columns);
        long start = state.getOptions().getStartTimestampOfTSData();
        return InfluxDBColumnReference.create(c, InfluxDBConstant.createUnsignedIntConstant(start));
    }

    @Override
    public InfluxDBExpression negatePredicate(InfluxDBExpression predicate) {
        return null;
    }

    @Override
    public InfluxDBExpression isNull(InfluxDBExpression expr) {
        return null;
    }

    @Override
    public List<InfluxDBExpression> generateOrderBys() {
        // order by columns
        List<InfluxDBColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<InfluxDBExpression> expressions = columnsForOrderBy.stream().map(column -> {
            return new InfluxDBColumnReference(column, null);
        }).collect(Collectors.toList());
        List<InfluxDBExpression> newOrderBys = new ArrayList<>();
        for (InfluxDBExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                InfluxDBOrderByTerm newExpr = new InfluxDBOrderByTerm(expr, InfluxDBOrderByTerm.InfluxDBOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
