package com.fuzzy.prometheus.oracle;


import com.fuzzy.SQLConnection;
import com.fuzzy.common.oracle.PivotedQuerySynthesisBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.prometheus.PrometheusErrors;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusRowValue;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusTables;
import com.fuzzy.prometheus.ast.PrometheusExpression;

import java.sql.SQLException;
import java.util.List;

public class PrometheusPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<PrometheusGlobalState, PrometheusRowValue, PrometheusExpression, SQLConnection> {

    private List<PrometheusExpression> fetchColumns;
    private List<PrometheusColumn> columns;

    public PrometheusPivotedQuerySynthesisOracle(PrometheusGlobalState globalState) throws SQLException {
        super(globalState);
        PrometheusErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        PrometheusSchema schema = globalState.getSchema();
        PrometheusTables randomFromTables = schema.getRandomTableNonEmptyTables();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

//        PrometheusSelect selectStatement = new PrometheusSelect();
//        selectStatement.setFromList(new ArrayList<>(Collections.singletonList(new PrometheusSchemaReference(schema))));
//
//        // 时间戳字段设置表信息(get(0)) -> 该数据库下任意表均有该字段
//        columns = randomFromTables.getColumns();
//        PrometheusColumn timeColumn = new PrometheusColumn(PrometheusValueStateConstant.TIME_COLUMN.getValue(), false, PrometheusDataType.INT64);
//        timeColumn.setTable(randomFromTables.getTables().get(0));
//        columns.add(timeColumn);
//        fetchColumns = columns.stream().map(c -> new PrometheusColumnReference(c, null)).collect(Collectors.toList());
//        // 划分cast和普通Column
//        int splitCastColumnIndex = globalState.getRandomly().getInteger(0, fetchColumns.size());
//        // cast Columns
//        selectStatement.setCastColumns(fetchColumns.subList(0, splitCastColumnIndex).stream().map(columnRef ->
//                new PrometheusCastOperation(columnRef, CastType.getRandom())).collect(Collectors.toList()));
//        // fetch Columns
//        selectStatement.setFetchColumns(fetchColumns.subList(splitCastColumnIndex, fetchColumns.size()));
//        PrometheusExpression whereClause = generateRectifiedExpression(columns, pivotRow);
//        selectStatement.setWhereClause(whereClause);
//        // TODO 仅当执行聚合函数时才支持group by
//        // 且limit语句存在时不支持group by
////        List<PrometheusExpression> groupByClause = generateGroupByClause(columns, pivotRow);
////        selectStatement.setGroupByExpressions(groupByClause);
//        PrometheusExpression limitClause = generateLimit();
//        selectStatement.setLimitClause(limitClause);
//        if (limitClause != null) {
//            PrometheusExpression offsetClause = generateOffset();
//            selectStatement.setOffsetClause(offsetClause);
//        }
//        List<PrometheusExpression> orderBy = new PrometheusExpressionGenerator(globalState).setColumns(columns)
//                .generateOrderBys();
//        selectStatement.setOrderByExpressions(orderBy);

        return new SQLQueryAdapter("sql", errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {

    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:

                break;
            case invalid:

                break;
            case success:

                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected String asSequenceString(PrometheusExpression expr) {
        return "";
    }

    private List<PrometheusExpression> generateGroupByClause(List<PrometheusColumn> columns, PrometheusRowValue rw) {
//        if (Randomly.getBoolean()) {
//            return columns.stream().map(c -> PrometheusColumnReference.create(c, rw.getValues().get(c)))
//                    .collect(Collectors.toList());
//        } else {
//            return Collections.emptyList();
//        }
        return null;
    }

//    private PrometheusConstant generateLimit() {
//        if (Randomly.getBoolean()) {
//            return PrometheusConstant.createIntConstant(Integer.MAX_VALUE);
//        } else {
//            return null;
//        }
//        return null;
//    }

    private PrometheusExpression generateOffset() {
//        if (Randomly.getBoolean()) {
//            return PrometheusConstant.createIntConstantNotAsBoolean(0);
//        } else {
//            return null;
//        }
        return null;
    }

    private PrometheusExpression generateRectifiedExpression(List<PrometheusColumn> columns, PrometheusRowValue rw) {
//        PrometheusExpression expression = new PrometheusExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
//                .generateExpression();
//        // 根据抽象语法树（AST）解释器的计算结果, 修改谓词, 使其查询结果必定包含pivot row
//        // is null生成全部去掉, true or false生成修改Not取反
//        PrometheusConstant expectedValue = expression.getExpectedValue();
//        PrometheusExpression result;
//        if (expectedValue.isNull()) {
//            result = new PrometheusUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
//        } else if (expectedValue.asBooleanNotNull()) {
//            result = expression;
//        } else {
//            result = new PrometheusUnaryPrefixOperation(expression, PrometheusUnaryPrefixOperator.NOT);
//        }
//        rectifiedPredicates.add(result);
//        return result;
        return null;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
//        StringBuilder sb = new StringBuilder();
//        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
//        sb.append(query.getUnterminatedQueryString());
//        sb.append(") as result WHERE ");
//        int i = 0;
//        for (PrometheusColumn c : columns) {
//            if (i++ != 0) {
//                sb.append(" AND ");
//            }
//            sb.append("result.");
//            sb.append("ref");
//            sb.append(i - 1);
//            if (pivotRow.getValues().get(c).isNull()) {
//                sb.append(" IS NULL");
//            } else {
//                sb.append(" = ");
//                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
//            }
//        }
//
//        String resultingQueryString = sb.toString();
//        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
        return null;
    }

    @Override
    protected String getExpectedValues(PrometheusExpression expr) {
//        return PrometheusVisitor.asExpectedValues(expr);
        return null;
    }
}
