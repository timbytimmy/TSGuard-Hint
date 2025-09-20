package com.fuzzy.griddb.oracle;


import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.TimeSeriesAlgebraFrameworkBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.tsaf.PredicationEquation;
import com.fuzzy.common.tsaf.QueryType;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.griddb.GridDBErrors;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema;
import com.fuzzy.griddb.GridDBSchema.GridDBColumn;
import com.fuzzy.griddb.GridDBSchema.GridDBRowValue;
import com.fuzzy.griddb.GridDBSchema.GridDBTable;
import com.fuzzy.griddb.GridDBSchema.GridDBTables;
import com.fuzzy.griddb.GridDBVisitor;
import com.fuzzy.griddb.ast.*;
import com.fuzzy.griddb.feedback.GridDBQuerySynthesisFeedbackManager;
import com.fuzzy.griddb.gen.GridDBExpressionGenerator;
import com.fuzzy.griddb.gen.GridDBInsertGenerator;
import com.fuzzy.griddb.gen.GridDBTimeExpressionGenerator;
import com.fuzzy.griddb.tsaf.enu.GridDBAggregationType;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class GridDBTSAFOracle
        extends TimeSeriesAlgebraFrameworkBase<GridDBGlobalState, GridDBRowValue, GridDBExpression, SQLConnection> {
    // TODO 插入数据空间概率分布
    // 数值和时间戳具备二元函数关系（能否知微见著，反映出各种不规律数值空间？）
    // 预期结果集由两者得出
    // TODO 完整插入所有数据后，随机更新某些时间戳数据（或者插入数据时间戳乱序，前者验证lsm树更新操作，后者验证时序数据库的合并排序）
    // TODO （时间戳和数值按列单独存储，所以两者之间的关系性不会影响到时序数据库查询？看目前是否有时序数据库压缩，查询操作考虑到多列相关性）
    private List<GridDBExpression> fetchColumns;
    private List<GridDBColumn> columns;
    private GridDBTable table;
    private GridDBExpression whereClause;
    GridDBSelect selectStatement;

    public GridDBTSAFOracle(GridDBGlobalState globalState) {
        super(globalState);
        GridDBErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        GridDBSchema schema = globalState.getSchema();
        GridDBTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<GridDBTable> tables = randomFromTables.getTables();
        selectStatement = new GridDBSelect();
        table = Randomly.fromList(tables);
        selectStatement.setSelectType(Randomly.fromOptions(GridDBSelect.SelectType.values()));
        // 注: TSAF和大多数时序数据库均不支持将time字段和其他字段进行比较、算术二元运算
        columns = randomFromTables.getColumns().stream()
                .filter(c -> !c.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName())
                        && !c.getName().equalsIgnoreCase(GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName()))
                .collect(Collectors.toList());
        selectStatement.setFromList(tables.stream().map(GridDBTableReference::new).collect(Collectors.toList()));

        QueryType queryType = Randomly.fromOptions(QueryType.BASE_QUERY, QueryType.TIME_WINDOW_QUERY);
        selectStatement.setQueryType(queryType);
        // 随机窗口查询测试
        if (queryType.isTimeWindowQuery()) generateTimeWindowClause(columns);
        // fetchColumns
        fetchColumns = columns.stream().map(c -> {
            String columnName = c.getName();
            if (queryType.isTimeWindowQuery())
                columnName = String.format("%s(%s)", GridDBAggregationType.getGridDBAggregationType(
                        selectStatement.getAggregationType()), columnName);
            return new GridDBColumnReference(new GridDBColumn(columnName, c.getType()), null);
        }).collect(Collectors.toList());
        String timeColumnName = GridDBConstantString.TIME_FIELD_NAME.getName();
        fetchColumns.add(new GridDBColumnReference(new GridDBColumn(timeColumnName,
                GridDBSchema.GridDBDataType.TIMESTAMP), null));
        selectStatement.setFetchColumns(fetchColumns);
        whereClause = generateExpression(columns);
        selectStatement.setWhereClause(whereClause);

        // Limit
        GridDBExpression limitClause = generateLimit();
        if (limitClause != null) {
            selectStatement.setLimitClause(limitClause);
            GridDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        // Order by
        List<GridDBExpression> orderBy = new GridDBExpressionGenerator(globalState).setColumns(
                Collections.singletonList(new GridDBSchema.GridDBColumn(timeColumnName,
                        GridDBSchema.GridDBDataType.TIMESTAMP))).generateOrderBys();
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
    protected TimeSeriesConstraint genColumnConstraint(GridDBExpression expr) {
        return GridDBVisitor.asConstraint(globalState.getDatabaseName(), table.getName(), expr,
                TableToNullValuesManager.getNullValues(globalState.getDatabaseName(), table.getName()));
    }

    @Override
    protected Map<Long, List<BigDecimal>> getExpectedValues(GridDBExpression expression) {
        // 联立方程式求解
        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        List<String> fetchColumnNames = columns.stream().map(AbstractTableColumn::getName).collect(Collectors.toList());
        TimeSeriesConstraint timeSeriesConstraint = GridDBVisitor.asConstraint(databaseName, tableName,
                expression, TableToNullValuesManager.getNullValues(databaseName, tableName));
        PredicationEquation predicationEquation = new PredicationEquation(timeSeriesConstraint, "equationName");
        return predicationEquation.genExpectedResultSet(databaseName, tableName, fetchColumnNames,
                globalState.getOptions().getStartTimestampOfTSData(),
                GridDBInsertGenerator.getLastTimestamp(databaseName, tableName),
                GridDBConstant.createFloatArithmeticTolerance());
    }

    @Override
    protected boolean verifyResultSet(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) {
        try {
            if (selectStatement.getQueryType().isTimeWindowQuery())
                return verifyTimeWindowQuery(expectedResultSet, result);
            else return verifyGeneralQuery(expectedResultSet, result);
        } catch (Exception e) {
            log.error("验证查询结果集和预期结果集等价性异常, e:", e);
            return false;
        }
    }

    private boolean verifyTimeWindowQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 对expectedResultSet进行聚合, 聚合结果按照时间戳作为Key, 进行进一步数值比较
        Map<Long, List<BigDecimal>> aggregationResultSet = null;
        if (!result.hasNext()) {
            if (!expectedResultSet.isEmpty())
                log.info("result不具备值, expectedResultSet: {}", expectedResultSet);
            return expectedResultSet.isEmpty();
        }

        // 获取窗口划分初始时间戳
        int timeColumnIndex = result.findColumn(GridDBConstantString.TIME_FIELD_NAME.getName());
        GridDBConstant timeConstant = getConstantFromResultSet(result, timeColumnIndex);

        long startTimestamp = timeConstant.getBigDecimalValue().longValue();
        String intervalVal = selectStatement.getIntervalValues().get(0);
        String slidingValue = selectStatement.getSlidingValue();
        long duration = globalState.transTimestampToPrecision(Long.parseLong(intervalVal) * 1000);
        long sliding = duration;

        // 窗口聚合 -> 预期结果集
        selectStatement.getAggregationType().setSingleValueFillZero(true);
        aggregationResultSet = selectStatement.getAggregationType()
                .apply(expectedResultSet, startTimestamp, duration, sliding);

        // 验证结果
        int rowCount = 0;
        do {
            timeColumnIndex = result.findColumn(GridDBConstantString.TIME_FIELD_NAME.getName());
            timeConstant = getConstantFromResultSet(result, timeColumnIndex);
            startTimestamp = timeConstant.getBigDecimalValue().longValue();

            // 多条时序
            boolean hasNullValue = false;
            for (int i = 0; i < fetchColumns.size() - 1; i++) {
                GridDBConstant constant = getConstantFromResultSet(result, i + 1);
                if (constant.isNull()) {
                    hasNullValue = true;
                    continue;
                }

                if (!aggregationResultSet.containsKey(startTimestamp)) {
                    log.info("预期结果集中不包含实际结果集时间戳, timestamp:{} actualValue:{} 聚合结果集:{} 原结果集:{}", startTimestamp,
                            constant.getBigDecimalValue(), aggregationResultSet, expectedResultSet);
                    logAggregationSet(aggregationResultSet);
                    return false;
                }
                GridDBConstant isEquals = constant.isEquals(
                        GridDBConstant.createBigDecimalConstant(aggregationResultSet.get(startTimestamp).get(i)));

                if (!isEquals.asBooleanNotNull()) {
                    log.info("预期结果集和实际结果集具体时间戳下数据异常, timestamp:{}, expectedValue:{}, actualValue:{}",
                            startTimestamp, aggregationResultSet.get(startTimestamp), constant.getBigDecimalValue());
                    logAggregationSet(aggregationResultSet);
                    return false;
                }
            }
            if (!hasNullValue) rowCount++;
        } while (result.hasNext());

        if (aggregationResultSet.size() != rowCount) {
            log.error("预期结果集和实际结果集数目不一致, expectedResultSet:{} resultSet:{}",
                    aggregationResultSet.size(), rowCount);
            logAggregationSet(aggregationResultSet);
            return false;
        }

        return true;
    }

    private boolean verifyGeneralQuery(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result)
            throws Exception {
        // 针对每行进行验证
        int rowCount = 0;
        while (result.hasNext()) {
            VerifyResultState verifyResult = verifyRowResult(expectedResultSet, result);
            if (verifyResult == VerifyResultState.FAIL) return false;
            else if (verifyResult == VerifyResultState.SUCCESS) rowCount++;
        }
        if (expectedResultSet.size() != rowCount) {
            log.error("预期结果集和实际结果集数目不一致, expectedResultSet:{} resultSet:{}",
                    expectedResultSet.size(), rowCount);
            return false;
        }
        return true;
    }

    private VerifyResultState verifyRowResult(Map<Long, List<BigDecimal>> expectedResultSet, DBValResultSet result) throws Exception {
        // time取最后一列
        GridDBColumn timeColumn =
                ((GridDBColumnReference) fetchColumns.get(fetchColumns.size() - 1)).getColumn();
        int timeColumnIndex = result.findColumn(timeColumn.getName().toLowerCase());
        GridDBConstant timeConstant = getConstantFromResultSet(result, timeColumnIndex);
        long timestamp = timeConstant.getBigDecimalValue().longValue();

        // 多列比较
        for (int i = 0; i < fetchColumns.size() - 1; i++) {
            GridDBColumn column = ((GridDBColumnReference) fetchColumns.get(i)).getColumn();
            int columnIndex = result.findColumn(column.getName().toLowerCase());
            GridDBConstant constant = getConstantFromResultSet(result, columnIndex);
            // 空值不进行评估
            if (constant.isNull()) return VerifyResultState.IS_NULL;

            if (!expectedResultSet.containsKey(timestamp)) {
                log.info("预期结果集中不包含实际结果集时间戳, timestamp:{}", timestamp);
                return VerifyResultState.FAIL;
            }
            GridDBConstant isEquals = constant.isEquals(
                    new GridDBConstant.GridDBBigDecimalConstant(expectedResultSet.get(timestamp).get(i)));
            if (isEquals.isNull()) throw new AssertionError();
            else if (!isEquals.asBooleanNotNull()) {
                log.info("预期结果集和实际结果集具体时间戳下数据异常, timestamp:{}, expectedValue:{}, actualValue:{}",
                        timestamp, expectedResultSet.get(timestamp), constant.getBigDecimalValue());
                return VerifyResultState.FAIL;
            }
        }
        return VerifyResultState.SUCCESS;
    }

    private enum VerifyResultState {
        SUCCESS, FAIL, IS_NULL
    }

    private void logAggregationSet(Map<Long, List<BigDecimal>> aggregationResultSet) {
        globalState.getState().getLocalState().log(String.format("aggregationResultSet size:%d %s",
                aggregationResultSet.size(), aggregationResultSet));
    }

    private GridDBConstant getConstantFromResultSet(DBValResultSet resultSet, int columnIndex) throws Exception {
        GridDBSchema.GridDBDataType dataType = GridDBSchema.GridDBDataType.getDataTypeByName(
                resultSet.getMetaData().getColumnTypeName(columnIndex));

        GridDBConstant constant;
        if (resultSet.getString(columnIndex) == null
                || resultSet.getString(columnIndex).equalsIgnoreCase("null")) {
            return GridDBConstant.createNullConstant();
        } else {
            Object value;
            switch (dataType) {
                case TIMESTAMP:
                    Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                    value = timestamp.getTime();
                    constant = GridDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                case INTEGER:
                case LONG:
                    value = resultSet.getLong(columnIndex);
                    constant = GridDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                case STRING:
                    value = resultSet.getString(columnIndex);
                    constant = GridDBConstant.createStringConstant((String) value);
                    break;
                case FLOAT:
                case DOUBLE:
                    value = resultSet.getDouble(columnIndex);
                    constant = GridDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                    break;
                default:
                    throw new AssertionError(dataType);
            }
        }
        if (selectStatement.getAggregationType() != null
                && GridDBAggregationType.getGridDBAggregationType(selectStatement.getAggregationType()).isFillZero()
                && constant.getBigDecimalValue().compareTo(BigDecimal.ZERO) == 0
                && dataType != GridDBSchema.GridDBDataType.TIMESTAMP)
            return GridDBConstant.createNullConstant();
        return constant;
    }

    private GridDBExpression generateExpression(List<GridDBColumn> columns) {
        GridDBExpression result = null;
        boolean reGenerateExpr;
        int regenerateCounter = 0;
        String predicateSequence = "";
        do {
            reGenerateExpr = false;
            try {
                GridDBExpression predicateExpression =
                        new GridDBExpressionGenerator(globalState).setColumns(columns).generateExpression();
                // 将表达式纠正为BOOLEAN类型
                GridDBExpression rectifiedPredicateExpression = predicateExpression;

                // 单列谓词 -> 重新生成
                if (!predicateExpression.getExpectedValue().isBoolean())
                    rectifiedPredicateExpression =
                            GridDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(predicateExpression);

                // add time column
                GridDBExpression timeExpression = new GridDBTimeExpressionGenerator(globalState).setColumns(
                        Collections.singletonList(new GridDBColumn(GridDBConstantString.TIME_FIELD_NAME.getName(),
                                GridDBSchema.GridDBDataType.TIMESTAMP))).generateExpression();
                // TIME
                if (selectStatement.getQueryType().isTimeWindowQuery()) {
                    timeExpression = new GridDBBinaryLogicalOperation(timeExpression, new GridDBBinaryComparisonOperation(
                            new GridDBColumnReference(new GridDBColumn(GridDBConstantString.TIME_FIELD_NAME.getName(),
                                    GridDBSchema.GridDBDataType.TIMESTAMP), GridDBConstant.createInt64Constant(0)),
                            GridDBConstant.createTimestamp(globalState.getOptions().getStartTimestampOfTSData()),
                            GridDBBinaryComparisonOperation.BinaryComparisonOperator.GREATER_EQUALS),
                            GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND);
                    timeExpression = new GridDBBinaryLogicalOperation(timeExpression, new GridDBBinaryComparisonOperation(
                            new GridDBColumnReference(new GridDBColumn(GridDBConstantString.TIME_FIELD_NAME.getName(),
                                    GridDBSchema.GridDBDataType.TIMESTAMP), GridDBConstant.createInt64Constant(0)),
                            GridDBConstant.createTimestamp(globalState.transTimestampToPrecision(new Date().getTime())),
                            GridDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS),
                            GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND);
                }

                result = new GridDBBinaryLogicalOperation(rectifiedPredicateExpression, timeExpression,
                        GridDBBinaryLogicalOperation.GridDBBinaryLogicalOperator.AND);
                log.info("Expression: {}", GridDBVisitor.asString(result));

                predicateSequence = GridDBVisitor.asString(rectifiedPredicateExpression, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && GridDBQuerySynthesisFeedbackManager.isRegenerateSequence(predicateSequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= GridDBQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        GridDBQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", predicateSequence));
                }
                // 更新概率表
                GridDBQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(predicateSequence);
            } catch (ReGenerateExpressionException e) {
                log.info("ReGenerateExpression: {}", e.getMessage());
                reGenerateExpr = true;
            }
        } while (reGenerateExpr);

        timePredicate = result;
        this.predicateSequence = predicateSequence;
        return result;
    }

    private void generateTimeWindowClause(List<GridDBColumn> columns) {
        selectStatement.setAggregationType(GridDBAggregationType.getRandomAggregationType());
        String timeUnit = "SECOND";
        long intervalVal = globalState.getRandomly().getLong(1, 10000);
        long intervalOffset = globalState.getRandomly().getLong(0, intervalVal);
//        long slidingVal = globalState.getRandomly().getLong(intervalVal / 100 + 1, intervalVal);

        List<String> intervals = new ArrayList<>();
        intervals.add(String.valueOf(intervalVal));
        if (intervalOffset != 0) intervals.add(String.valueOf(intervalOffset));
        selectStatement.setIntervalValues(intervals);
//        if (slidingVal != 0) selectStatement.setSlidingValue(String.format("%d%s", slidingVal, timeUnit));
    }

    private List<GridDBExpression> generateGroupByClause(List<GridDBColumn> columns, GridDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> GridDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private GridDBConstant generateLimit() {
        if (Randomly.getBoolean()) return GridDBConstant.createInt32Constant(Integer.MAX_VALUE);
        else return null;
    }

    private GridDBExpression generateOffset() {
        if (Randomly.getBoolean()) return GridDBConstant.createInt32Constant(0);
        else return null;
    }
}
