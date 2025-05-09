package com.fuzzy.TDengine.oracle;


import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.TDengine.TDengineErrors;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.TDengineSchema.TDengineColumn;
import com.fuzzy.TDengine.TDengineSchema.TDengineRowValue;
import com.fuzzy.TDengine.TDengineSchema.TDengineTable;
import com.fuzzy.TDengine.TDengineSchema.TDengineTables;
import com.fuzzy.TDengine.TDengineVisitor;
import com.fuzzy.TDengine.ast.*;
import com.fuzzy.TDengine.ast.TDengineUnaryPostfixOperation.UnaryPostfixOperator;
import com.fuzzy.TDengine.feedback.TDengineQuerySynthesisFeedbackManager;
import com.fuzzy.TDengine.gen.TDengineExpressionGenerator;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.PivotedQuerySynthesisBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TDenginePivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<TDengineGlobalState, TDengineRowValue, TDengineExpression, SQLConnection> {

    private List<TDengineExpression> fetchColumns;
    private List<TDengineColumn> columns;

    public TDenginePivotedQuerySynthesisOracle(TDengineGlobalState globalState) throws SQLException {
        super(globalState);
        TDengineErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        TDengineSchema schema = globalState.getSchema();
        TDengineTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<TDengineTable> tables = randomFromTables.getTables();

        TDengineSelect selectStatement = new TDengineSelect();
        selectStatement.setSelectType(Randomly.fromOptions(TDengineSelect.SelectType.values()));
        // TODO 不将时间戳列纳入 计算列 范围
        columns = randomFromTables.getColumns().stream()
                .filter(c -> !c.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName()))
                .collect(Collectors.toList());
        pivotRow = randomFromTables.getRandomRowValue(globalState);

        selectStatement.setFromList(tables.stream().map(t -> new TDengineTableReference(t)).collect(Collectors.toList()));
        fetchColumns = randomFromTables.getColumns().stream()
                .map(c -> new TDengineColumnReference(c, null)).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        TDengineExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        // TODO group by
//        List<TDengineExpression> groupByClause = generateGroupByClause(columns, pivotRow);
//        selectStatement.setGroupByExpressions(groupByClause);
        TDengineExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            TDengineExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        // TODO order by -> Column ambiguously defined
        List<TDengineExpression> orderBy = new TDengineExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(TDengineVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        TDengineQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                TDengineQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                TDengineQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                TDengineQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected String asSequenceString(TDengineExpression expr) {
        return TDengineVisitor.asString(expr, true);
    }

    private List<TDengineExpression> generateGroupByClause(List<TDengineColumn> columns, TDengineRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> TDengineColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private TDengineConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return TDengineConstant.createInt32Constant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private TDengineExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return TDengineConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private TDengineExpression generateRectifiedExpression(List<TDengineColumn> columns, TDengineRowValue rw) {
        TDengineExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        do {
            reGenerateExpr = false;
            try {
                TDengineExpression expression = new TDengineExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                        .generateExpression();
                TDengineConstant expectedValue = expression.getExpectedValue();
                if (expectedValue.isNull()) {
                    result = new TDengineUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
                } else if (expectedValue.asBooleanNotNull()) {
                    result = expression;
                } else {
                    result = TDengineUnaryNotPrefixOperation.getNotUnaryPrefixOperation(expression);
                }

                if (globalState.getOptions().useSyntaxValidator()) result.checkSyntax();
                String sequence = TDengineVisitor.asString(result, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && TDengineQuerySynthesisFeedbackManager.isRegenerateSequence(sequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= TDengineQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        TDengineQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", sequence));
                }

                // 更新概率表
                TDengineQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(sequence);
            } catch (ReGenerateExpressionException e) {
                log.info("ReGenerateExpression: {}", e.getMessage());
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        // TDengine不支持子查询，将PQS子查询结构进行改造
        StringBuilder sb = new StringBuilder();
        String[] querySegment = query.getUnterminatedQueryString().split("WHERE");
        assert querySegment.length == 2;

        sb.append(querySegment[0]);
        sb.append("WHERE ");
        int i = 0;
        for (TDengineColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
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
    protected String getExpectedValues(TDengineExpression expr) {
        return TDengineVisitor.asExpectedValues(expr);
    }
}
