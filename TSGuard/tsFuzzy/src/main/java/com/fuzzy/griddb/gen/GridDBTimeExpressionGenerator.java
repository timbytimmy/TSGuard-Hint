package com.fuzzy.griddb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.gen.UntypedExpressionGenerator;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema.GridDBColumn;
import com.fuzzy.griddb.ast.*;
import com.fuzzy.griddb.ast.GridDBOrderByTerm.GridDBOrder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class GridDBTimeExpressionGenerator extends UntypedExpressionGenerator<GridDBExpression, GridDBColumn> {

    private static final int maxDepth = 1;
    private final GridDBGlobalState state;
    private boolean hasTimeColumn = false;

    public GridDBTimeExpressionGenerator(GridDBGlobalState state) {
        this.state = state;
    }

    private enum Actions {
        COLUMN, LITERAL,
        BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON_OPERATION,
        IN_OPERATION, BETWEEN_OPERATOR;

        public static Actions[] getLogicActions() {
            return new Actions[]{Actions.BINARY_LOGICAL_OPERATOR, Actions.BINARY_COMPARISON_OPERATION,
                    Actions.IN_OPERATION, Actions.BETWEEN_OPERATOR};
        }
    }

    @Override
    protected GridDBExpression generateExpression(int depth) {
        if (depth >= maxDepth) {
            if (hasTimeColumn) return generateLeafNode();
            hasTimeColumn = true;
            return generateColumn();
        }

        Actions action;
        if (depth == 0) action = Randomly.fromOptions(Actions.getLogicActions());
        else action = Randomly.fromOptions(Actions.values());
        GridDBExpression expression;
        switch (action) {
            case COLUMN:
                expression = generateColumn();
                break;
            case LITERAL:
                expression = generateConstant();
                break;
            case BINARY_LOGICAL_OPERATOR:
                // 非Boolean值操作符不允许单独出现至AND等后面
                if (depth >= maxDepth - 1) expression = generateExpression(depth);
                else expression = new GridDBBinaryLogicalOperation(
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.getRandom());
                break;
            case BINARY_COMPARISON_OPERATION:
                expression = new GridDBBinaryComparisonOperation(
                        generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        GridDBBinaryComparisonOperation.BinaryComparisonOperator.getRandom());
                break;
            case BETWEEN_OPERATOR:
                expression = new GridDBBetweenOperation(generateExpression(depth + 1),
                        generateExpression(depth + 1),
                        generateExpression(depth + 1), false);
                break;
            case IN_OPERATION:
                GridDBExpression expr = generateExpression(depth + 1);
                List<GridDBExpression> rightList = new ArrayList<>();
                for (int i = 0; i < 1 + Randomly.smallNumber(); i++)
                    rightList.add(generateExpression(depth + 1));
                expression = new GridDBInOperation(expr, rightList, Randomly.getBoolean());
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
        return null;
    }

    @Override
    public GridDBExpression generateConstant() {
        return GridDBConstant.createTimestamp(state.getRandomTimestamp());
    }

    @Override
    protected GridDBExpression generateColumn() {
        GridDBColumn c = Randomly.fromList(columns);
        return GridDBColumnReference.create(c, GridDBConstant.createInt64Constant(
                state.getOptions().getStartTimestampOfTSData()));
    }

    @Override
    public GridDBExpression negatePredicate(GridDBExpression predicate) {
        return null;
    }

    @Override
    public GridDBExpression isNull(GridDBExpression expr) {
        return null;
    }

    @Override
    public List<GridDBExpression> generateOrderBys() {
        // order by columns
        List<GridDBColumn> columnsForOrderBy = Randomly.nonEmptySubset(columns);
        List<GridDBExpression> expressions = columnsForOrderBy.stream().map(column -> {
            return new GridDBColumnReference(column, null);
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
