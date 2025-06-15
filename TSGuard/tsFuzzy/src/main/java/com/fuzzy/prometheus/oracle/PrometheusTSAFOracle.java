package com.fuzzy.prometheus.oracle;


import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.TimeSeriesAlgebraFrameworkBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.QueryType;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.prometheus.PrometheusErrors;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusTable;
import com.fuzzy.prometheus.PrometheusVisitor;
import com.fuzzy.prometheus.ast.*;
import com.fuzzy.prometheus.feedback.PrometheusQuerySynthesisFeedbackManager;
import com.fuzzy.prometheus.gen.PrometheusExpressionGenerator;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class PrometheusTSAFOracle
        extends TimeSeriesAlgebraFrameworkBase<PrometheusGlobalState, PrometheusExpression, SQLConnection> {

    private List<PrometheusExpression> fetchColumns;
    private List<PrometheusColumn> columns;
    private PrometheusTable table;
    private PrometheusExpression whereClause;
    PrometheusSelect selectStatement;

    public PrometheusTSAFOracle(PrometheusGlobalState globalState) {
        super(globalState);
        PrometheusErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        PrometheusSchema schema = globalState.getSchema();
        PrometheusSchema.PrometheusTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<PrometheusSchema.PrometheusTable> tables = randomFromTables.getTables();
        selectStatement = new PrometheusSelect();
        table = Randomly.fromList(tables);
        selectStatement.setSelectType(Randomly.fromOptions(PrometheusSelect.SelectType.values()));
        // 注: TSAF和大多数时序数据库均不支持将time字段和其他字段进行比较、算术二元运算
//        columns = randomFromTables.getColumns().stream()
//                .filter(c -> !c.getName().equalsIgnoreCase(PrometheusConstantString.TIME_FIELD_NAME.getName())
//                        && !c.getName().equalsIgnoreCase(PrometheusConstantString.DEVICE_ID_COLUMN_NAME.getName()))
//                .collect(Collectors.toList());
        columns = randomFromTables.getColumns();
        selectStatement.setFromList(tables.stream().map(PrometheusTableReference::new).collect(Collectors.toList()));

        // TODO
//        QueryType queryType = Randomly.fromOptions(QueryType.values());
        QueryType queryType = Randomly.fromOptions(QueryType.BASE_QUERY);
        whereClause = generateExpression(columns);
        selectStatement.setWhereClause(whereClause);
        selectStatement.setQueryType(queryType);

        // TODO
//        if (queryType.isTimeWindowQuery()) {
//            // 随机窗口查询测试
//            generateTimeWindowClause(columns);
//        }
//        // TimeSeries Function
//        else if (queryType.isTimeSeriesFunction()) {
//            // 随机窗口查询测试
//            selectStatement.setTimeSeriesFunction(PrometheusTimeSeriesFunc.getRandomFunction(
//                    columns, globalState.getRandomly()));
//        }

        // fetchColumns
//        fetchColumns = columns.stream().map(c -> {
//            String columnName = c.getName();
//            if (queryType.isTimeWindowQuery())
//                columnName = String.format("%s(%s)", selectStatement.getAggregationType(), columnName);
//            else if (queryType.isTimeSeriesFunction())
//                columnName = selectStatement.getTimeSeriesFunction().combinedArgs(columnName);
//            return new PrometheusColumnReference(new PrometheusSchema.PrometheusColumn(columnName, c.isTag(), c.getType()), null);
//        }).collect(Collectors.toList());
        fetchColumns = columns.stream().map(c -> new PrometheusColumnReference(c, null))
                .collect(Collectors.toList());
//        String timeColumnName = queryType.isTimeWindowQuery() ? PrometheusConstantString.W_START_TIME_COLUMN_NAME.getName() :
//                PrometheusConstantString.TIME_FIELD_NAME.getName();
//        fetchColumns.add(new PrometheusColumnReference(new PrometheusSchema.PrometheusColumn(timeColumnName, false,
//                PrometheusSchema.PrometheusDataType.TIMESTAMP), null));
        selectStatement.setFetchColumns(fetchColumns);

        // ORDER BY
        // Prometheus distinct 不支持和order by time结合
//        if (!(selectStatement.getQueryType().isTimeSeriesFunction()
//                && selectStatement.getFromOptions() == PrometheusSelect.SelectType.DISTINCT)) {
//            List<PrometheusExpression> orderBy = new PrometheusExpressionGenerator(globalState).setColumns(
//                    Collections.singletonList(new PrometheusSchema.PrometheusColumn(timeColumnName, false,
//                            PrometheusSchema.PrometheusDataType.TIMESTAMP))).generateOrderBys();
//            selectStatement.setOrderByExpressions(orderBy);
//        }
        return new SQLQueryAdapter(PrometheusVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
//        PrometheusQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
//        switch (queryType) {
//            case error:
//                PrometheusQuerySynthesisFeedbackManager.incrementErrorQueryCount();
//                break;
//            case invalid:
//                PrometheusQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
//                break;
//            case success:
//                PrometheusQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
//                break;
//            default:
//                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
//        }
    }

    @Override
    protected TimeSeriesConstraint genColumnConstraint(PrometheusExpression expr) {
//        return PrometheusVisitor.asConstraint(globalState.getDatabaseName(), table.getName(), expr,
//                TableToNullValuesManager.getNullValues(globalState.getDatabaseName(), table.getName()));
        return null;
    }

    @Override
    protected Map<Long, List<BigDecimal>> getExpectedValues(PrometheusExpression expression) {
        // 联立方程式求解
//        String databaseName = globalState.getDatabaseName();
//        String tableName = table.getName();
//        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
//        TimeSeriesConstraint timeSeriesConstraint = PrometheusVisitor.asConstraint(databaseName, tableName,
//                expression, TableToNullValuesManager.getNullValues(databaseName, tableName));
//        PredicationEquation predicationEquation = new PredicationEquation(timeSeriesConstraint, "equationName");
//        return predicationEquation.genExpectedResultSet(databaseName, tableName, fetchColumnNames,
//                globalState.getOptions().getStartTimestampOfTSData(),
//                PrometheusInsertGenerator.getLastTimestamp(databaseName, tableName),
//                PrometheusConstant.createFloatArithmeticTolerance());
        return null;
    }

    @Override
    protected boolean verifyResultSet(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) {
//        try {
//            if (selectStatement.getQueryType().isTimeSeriesFunction()
//                    || selectStatement.getQueryType().isTimeWindowQuery())
//                return verifyTimeWindowQuery(expectedResultSet, result);
//            else return verifyGeneralQuery(expectedResultSet, result);
//        } catch (Exception e) {
//            log.error("验证查询结果集和预期结果集等价性异常, e:", e);
//            return false;
//        }
        return true;
    }

    private boolean verifyTimeWindowQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
//        // 对expectedResultSet进行聚合, 聚合结果按照时间戳作为Key, 进行进一步数值比较
//        PrometheusResultSet PrometheusResultSet = (PrometheusResultSet) result;
//        Map<Long, List<BigDecimal>> resultSet = null;
//        if (!result.hasNext()) {
//            if (selectStatement.getQueryType().isTimeWindowQuery()) return expectedResultSet.isEmpty();
//            else
//                return PrometheusTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet).isEmpty();
//        }
//
//        // 窗口聚合 -> 预期结果集
//        if (selectStatement.getQueryType().isTimeWindowQuery()) {
//            PrometheusColumn timeColumn = new PrometheusColumn(PrometheusValueStateConstant.TIME_FIELD.getValue(),
//                    false, PrometheusDataType.TIMESTAMP);
//            int timeIndex = PrometheusResultSet.findColumn(timeColumn.getName());
//            // 获取窗口划分初始时间戳
//            long startTimestamp = getConstantFromResultSet(PrometheusResultSet.getCurrentValue().getValues().get(0),
//                    timeColumn.getType(), timeIndex).getBigDecimalValue().longValue();
//            String intervalVal = selectStatement.getIntervalValues().get(0);
//            long duration = globalState.transTimestampToPrecision(
//                    Long.parseLong(intervalVal.substring(0, intervalVal.length() - 1)) * 1000);
//            resultSet = selectStatement.getAggregationType()
//                    .apply(expectedResultSet, startTimestamp, duration, duration);
//        } else if (selectStatement.getQueryType().isTimeSeriesFunction())
//            resultSet = PrometheusTimeSeriesFunc.apply(selectStatement.getTimeSeriesFunction(), expectedResultSet);
//
//        // 验证结果
//        result.resetCursor();
//        boolean verifyRes = verifyGeneralQuery(resultSet, result);
//        if (!verifyRes) logAggregationSet(resultSet);
//        return verifyRes;
        return true;
    }

    private boolean verifyGeneralQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 针对每行进行验证
        while (result.hasNext()) {
            if (verifyRowResult(expectedResultSet, result) == VerifyResultState.FAIL) return false;
        }

        return true;
    }

    private VerifyResultState verifyRowResult(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
//        getExpectedValues(whereClause);
//        PrometheusResultSet PrometheusResultSet = (PrometheusResultSet) result;
//        PrometheusColumn timeColumn = new PrometheusColumn(PrometheusValueStateConstant.TIME_FIELD.getValue(),
//                false, PrometheusDataType.TIMESTAMP);
//        int timeIndex = PrometheusResultSet.findColumn(timeColumn.getName());
//
//        for (int i = 0; i < fetchColumns.size(); i++) {
//            // 评估整条时间序列
//            PrometheusColumn dataColumn = ((PrometheusColumnReference) fetchColumns.get(i)).getColumn();
//
//            // 聚合函数列名取聚合函数名
//            String columnName = PrometheusValueStateConstant.REF.getValue() + i;
//            int columnIndex = PrometheusResultSet.findColumn(columnName);
//            PrometheusSeries currentValue = PrometheusResultSet.getCurrentValue();
//            int nullValueCount = 0;
//            for (List<String> stringValues : currentValue.getValues()) {
//                PrometheusConstant dataConstant = getConstantFromResultSet(stringValues, dataColumn.getType(), columnIndex);
//                if (dataConstant.isNull()) {
//                    nullValueCount++;
//                    continue;
//                }
//                long timestamp = getConstantFromResultSet(stringValues, timeColumn.getType(), timeIndex)
//                        .getBigDecimalValue().longValue();
//
//                if (!expectedResultSet.containsKey(timestamp)) {
//                    log.error("预期结果集中不包含实际结果集时间戳, timestamp:{}", timestamp);
//                    return VerifyResultState.FAIL;
//                }
//                PrometheusConstant isEquals = dataConstant.isEquals(
//                        new PrometheusConstant.PrometheusBigDecimalConstant(expectedResultSet.get(timestamp).get(i)));
//                if (isEquals.isNull()) throw new AssertionError();
//                else if (!isEquals.asBooleanNotNull()) {
//                    log.error("预期结果集和实际结果集具体时间戳下数据异常, timestamp:{}, expectedValue:{}, actualValue:{}",
//                            timestamp, expectedResultSet.get(timestamp), dataConstant.getBigDecimalValue());
//                    return VerifyResultState.FAIL;
//                }
//            }
//            // 数目
//            if (expectedResultSet.size() != currentValue.getValues().size() - nullValueCount) {
//                log.error("预期结果集和实际结果集数目不一致, expectedResultSetSize:{} rowCount:{} nullValueCount:{}",
//                        expectedResultSet.size(), currentValue.getValues().size(), nullValueCount);
//                return VerifyResultState.FAIL;
//            }
//        }
//        return VerifyResultState.SUCCESS;
        return null;
    }

    private enum VerifyResultState {
        SUCCESS, FAIL, IS_NULL
    }

    private void logAggregationSet(Map<Long, List<BigDecimal>> aggregationResultSet) {
        globalState.getState().getLocalState().log(String.format("aggregationResultSet size:%d %s",
                aggregationResultSet.size(), aggregationResultSet));
    }

    //    private PrometheusConstant getConstantFromResultSet(List<String> resultSet, PrometheusDataType dataType,
//                                                      int columnIndex) {
//        if (resultSet.get(columnIndex).equalsIgnoreCase("null")) return PrometheusConstant.createNullConstant();
//
//        PrometheusConstant constant;
//        switch (dataType) {
//            case BOOLEAN:
//                constant = PrometheusConstant.createBoolean(
//                        resultSet.get(columnIndex).equalsIgnoreCase("true"));
//                break;
//            case STRING:
//                constant = PrometheusConstant.createSingleQuotesStringConstant(resultSet.get(columnIndex));
//                break;
//            case INT:
//            case UINT:
//            case FLOAT:
//            case BIGDECIMAL:
//                constant = PrometheusConstant.createBigDecimalConstant(
//                        new BigDecimal(resultSet.get(columnIndex)));
//                break;
//            case TIMESTAMP:
//                // 时间格式 -> 直接返回, 不再进行空值判定
//                constant = PrometheusConstant.createBigDecimalConstant(
//                        new BigDecimal(globalState.transDateToTimestamp(resultSet.get(columnIndex))));
//                break;
//            default:
//                throw new AssertionError(dataType);
//        }
//        if (selectStatement.getAggregationType() != null
//                && PrometheusAggregationType.getPrometheusAggregationType(selectStatement.getAggregationType()).isFillZero()
//                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
//                && dataType != PrometheusDataType.TIMESTAMP)
//            return PrometheusConstant.createNullConstant();
//        return constant;
//    }
//
    private PrometheusExpression generateExpression(List<PrometheusSchema.PrometheusColumn> columns) {
        // TODO 生成 INT 或者 DOUBLE 值进行表达式测试
        PrometheusExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                PrometheusExpression predicateExpression =
                        new PrometheusExpressionGenerator(globalState).setColumns(columns).generateExpression();
                // 将表达式纠正为BOOLEAN类型
                PrometheusExpression rectifiedPredicateExpression = predicateExpression;

                // 单列谓词 -> 重新生成
//                if (!predicateExpression.getExpectedValue().isBoolean()) {
//                    rectifiedPredicateExpression =
//                            PrometheusUnaryNotPrefixOperation.getNotUnaryPrefixOperation(predicateExpression);
//                }

                // add time column
//                PrometheusExpression timeExpression = new PrometheusTimeExpressionGenerator(globalState).setColumns(
//                        Collections.singletonList(new PrometheusSchema.PrometheusColumn(PrometheusConstantString.TIME_FIELD_NAME.getName(),
//                                false, PrometheusSchema.PrometheusDataType.TIMESTAMP))).generateExpression();
                PrometheusExpression timeExpression = null;

                result = new PrometheusBinaryLogicalOperation(rectifiedPredicateExpression, timeExpression,
                        PrometheusBinaryLogicalOperation.PrometheusBinaryLogicalOperator.AND);
//                log.info("Expression: {}", PrometheusVisitor.asString(result));

                predicateSequence = PrometheusVisitor.asString(rectifiedPredicateExpression, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && PrometheusQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= PrometheusQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        PrometheusQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", predicateSequence));
                }
                // 更新概率表
                PrometheusQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        timePredicate = result;
        this.predicateSequence = predicateSequence;
        return result;
    }
//
//    private void generateTimeWindowClause() {
//        selectStatement.setAggregationType(PrometheusAggregationType.getRandomAggregationType());
//
//        List<String> intervals = new ArrayList<>();
//        // TODO 采样点前后1000s 取决于采样点endStartTimestamp
//        // TODO 时间范围
//        long timestampInterval = 1000 * 1000000L * 1000;
//        String timeUnit = "s";
//        long windowSize = globalState.getRandomly().getLong(100000, timestampInterval / 1000);
//        // TODO OFFSETSIZE 数值应用后返回大量无效值
////        long offsetSize = globalState.getRandomly().getLong(100000, timestampInterval / 1000);
//        intervals.add(String.format(String.format("%d%s", windowSize, timeUnit)));
////        intervals.add(String.format(String.format("%d%s", offsetSize, timeUnit)));
//        selectStatement.setIntervalValues(intervals);
//    }
//
//    private List<PrometheusExpression> generateGroupByClause(List<PrometheusColumn> columns, PrometheusRowValue rw) {
//        if (Randomly.getBoolean()) {
//            return columns.stream().map(c -> PrometheusColumnReference.create(c, rw.getValues().get(c)))
//                    .collect(Collectors.toList());
//        } else {
//            return Collections.emptyList();
//        }
//    }
}
