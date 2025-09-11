package com.fuzzy.griddb.oracle;


import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.PivotedQuerySynthesisBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.griddb.GridDBErrors;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema;
import com.fuzzy.griddb.GridDBSchema.GridDBColumn;
import com.fuzzy.griddb.GridDBSchema.GridDBRowValue;
import com.fuzzy.griddb.GridDBSchema.GridDBTables;
import com.fuzzy.griddb.GridDBVisitor;
import com.fuzzy.griddb.ast.*;
import com.fuzzy.griddb.feedback.GridDBQuerySynthesisFeedbackManager;
import com.fuzzy.griddb.gen.GridDBExpressionGenerator;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class GridDBPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<GridDBGlobalState, GridDBRowValue, GridDBExpression, SQLConnection> {
    private List<GridDBExpression> fetchColumns;
    private List<GridDBColumn> columns;

    public GridDBPivotedQuerySynthesisOracle(GridDBGlobalState globalState) throws SQLException {
        super(globalState);
        GridDBErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        GridDBSchema schema = globalState.getSchema();
        GridDBTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<GridDBSchema.GridDBTable> tables = randomFromTables.getTables();

        GridDBSelect selectStatement = new GridDBSelect();
        selectStatement.setSelectType(Randomly.fromOptions(GridDBSelect.SelectType.values()));
        columns = randomFromTables.getColumns().stream()
                .filter(c -> !c.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName()))
                .collect(Collectors.toList());
        pivotRow = randomFromTables.getRandomRowValue(globalState);

        selectStatement.setFromList(tables.stream().map(GridDBTableReference::new).collect(Collectors.toList()));
        fetchColumns = randomFromTables.getColumns().stream()
                .map(c -> new GridDBColumnReference(c, null)).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        GridDBExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        GridDBExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            GridDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<GridDBExpression> orderBy = new GridDBExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(GridDBVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        GridDBQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                GridDBQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                GridDBQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                GridDBQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected String asSequenceString(GridDBExpression expr) {
        return GridDBVisitor.asString(expr, true);
    }

    private GridDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return GridDBConstant.createInt32Constant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private GridDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return GridDBConstant.createInt32Constant(0);
        } else {
            return null;
        }
    }

    private GridDBExpression generateRectifiedExpression(List<GridDBColumn> columns, GridDBRowValue rw) {
        GridDBExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        do {
            reGenerateExpr = false;
            try {
                GridDBExpression expression = new GridDBExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                        .generateExpression();
                GridDBConstant expectedValue = expression.getExpectedValue();

                if (expectedValue.isNull()) {
                    result = new GridDBUnaryPostfixOperation(expression,
                            GridDBUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
                } else if (expectedValue.asBooleanNotNull()) {
                    result = expression;
                } else {
                    result = GridDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(expression);
                }
                if (globalState.getOptions().useSyntaxValidator()) result.checkSyntax();

                String sequence = GridDBVisitor.asString(result, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && GridDBQuerySynthesisFeedbackManager.isRegenerateSequence(sequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= GridDBQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        GridDBQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", sequence));
                }

                // 更新概率表
                GridDBQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(sequence);
            } catch (ReGenerateExpressionException e) {
                log.info("ReGenerateExpression: {}", e.getMessage());
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) {
        StringBuilder sb = new StringBuilder();
        String[] querySegment = query.getUnterminatedQueryString().split("WHERE");
        assert querySegment.length == 2;

        sb.append(querySegment[0]);
        sb.append("WHERE ");
        int i = 0;
        for (GridDBColumn c : columns) {
            if (i++ != 0) sb.append(" AND ");
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) sb.append(" IS NULL");
            else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        sb.append(" AND");
        sb.append(querySegment[1]);

        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
    }

    @Override
    protected String getExpectedValues(GridDBExpression expr) {
        return GridDBVisitor.asExpectedValues(expr);
    }
}
