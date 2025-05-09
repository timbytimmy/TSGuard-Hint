package com.fuzzy.influxdb.oracle;

import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.PivotedQuerySynthesisBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.influxdb.InfluxDBErrors;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBRowValue;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTables;
import com.fuzzy.influxdb.InfluxDBVisitor;
import com.fuzzy.influxdb.ast.*;
import com.fuzzy.influxdb.ast.InfluxDBConstant.InfluxDBIntConstant;
import com.fuzzy.influxdb.feedback.InfluxDBQuerySynthesisFeedbackManager;
import com.fuzzy.influxdb.gen.InfluxDBExpressionGenerator;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class InfluxDBPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<InfluxDBGlobalState, InfluxDBRowValue, InfluxDBExpression, SQLConnection> {

    private List<InfluxDBExpression> fetchColumns;
    private List<InfluxDBColumn> columns;
    private List<InfluxDBExpression> orderByForMainQuery;

    public InfluxDBPivotedQuerySynthesisOracle(InfluxDBGlobalState globalState) {
        super(globalState);
        InfluxDBErrors.addExpressionErrors(errors);
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("q=SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedAndNoQEqualQueryString());
        sb.append(") WHERE ");
        List<InfluxDBColumn> influxDBColumns = fetchColumns.stream().map(c -> {
            InfluxDBColumnReference columnReference = (InfluxDBColumnReference) c;
            return columnReference.getColumn();
        }).collect(Collectors.toList());
        for (int i = 0; i < influxDBColumns.size(); i++) {
            InfluxDBColumn c = influxDBColumns.get(i);
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("ref");
            sb.append(i - 1);
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                if (pivotRow.getValues().get(c) instanceof InfluxDBIntConstant)
                    sb.append(pivotRow.getValues().get(c).getInt());
                else
                    sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        if (!orderByForMainQuery.isEmpty()) {
            sb.append(" ORDER BY ");
            List<InfluxDBExpression> orderBys = orderByForMainQuery;
            for (int j = 0; j < orderBys.size(); j++) {
                if (j != 0) {
                    sb.append(", ");
                }
                sb.append(InfluxDBVisitor.asString(orderByForMainQuery.get(j)));
            }
        }

        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
    }

    @Override
    protected Query<SQLConnection> getRectifiedQuery() throws Exception {
        InfluxDBTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        InfluxDBTable table = randomFromTables.getTables().get(0);

        InfluxDBSelect selectStatement = new InfluxDBSelect();
        // TODO influxDB不含selectType -> 但支持子查询
        // selectStatement.setSelectType(Randomly.fromOptions(InfluxDBSelect.SelectType.values()));
        pivotRow = randomFromTables.getRandomRowValue(globalState);

        selectStatement.setFromList(Collections.singletonList(new InfluxDBTableReference(table)));

        // InfluxDB要求Tag在Field后面
        // where条件中表需和 pivot 一致, 且属于单一张表, 故对columns进行表限定(随机选择单张表列字段)
        columns = table.getColumns();
        fetchColumns = columns.stream().map(c -> new InfluxDBColumnReference(c, null))
                .sorted(Comparator.comparing(c -> c.getColumn().isTag()))
                .collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        InfluxDBExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<InfluxDBExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        InfluxDBExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            InfluxDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        // 仅支持order by time, 其他字段不识别的加入至errors中
        List<InfluxDBExpression> orderBy = new InfluxDBExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);
        orderByForMainQuery = orderBy;

        errors.add("Unsupported constant operand: NULL");
        return new SQLQueryAdapter(InfluxDBVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        InfluxDBQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                InfluxDBQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                InfluxDBQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                InfluxDBQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected String asSequenceString(InfluxDBExpression expr) {
        return InfluxDBVisitor.asString(expr, true);
    }

    @Override
    protected String getExpectedValues(InfluxDBExpression expr) {
        return InfluxDBVisitor.asExpectedValues(expr);
    }

    private InfluxDBExpression generateRectifiedExpression(List<InfluxDBColumn> columns, InfluxDBRowValue rw) {
        InfluxDBExpression result = null;
        int regenerateCounter = 0;
        boolean reGenerateExpr;
        do {
            try {
                reGenerateExpr = false;
                InfluxDBExpression expression = new InfluxDBExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                        .generateExpression();
                log.info("谓词表达式: {}", InfluxDBVisitor.asString(expression));
                InfluxDBConstant expectedValue = expression.getExpectedValue();
                if (expectedValue.isNull()) {
                    throw new AssertionError("InfluxDB不支持Null");
                } else if (expectedValue.asBooleanNotNull()) {
                    result = expression;
                } else {
                    result = InfluxDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(expression);
                }

                String sequence = InfluxDBVisitor.asString(result, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && InfluxDBQuerySynthesisFeedbackManager.isRegenerateSequence(sequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= InfluxDBQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        InfluxDBQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", sequence));
                }

                // 更新概率表
                InfluxDBQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(sequence);
            } catch (ReGenerateExpressionException e) {
                log.info("ReGenerateExpression: {}", e.getMessage());
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        rectifiedPredicates.add(result);
        return result;
    }

    private List<InfluxDBExpression> generateGroupByClause(List<InfluxDBColumn> columns, InfluxDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> InfluxDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private InfluxDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return InfluxDBConstant.createIntConstant(Integer.MAX_VALUE, true, false);
        } else {
            return null;
        }
    }

    private InfluxDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return InfluxDBConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }
}
