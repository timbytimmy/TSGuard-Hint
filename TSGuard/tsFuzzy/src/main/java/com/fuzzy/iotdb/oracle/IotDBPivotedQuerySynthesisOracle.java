package com.fuzzy.iotdb.oracle;


import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.oracle.PivotedQuerySynthesisBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.iotdb.IotDBErrors;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema;
import com.fuzzy.iotdb.IotDBSchema.IotDBColumn;
import com.fuzzy.iotdb.IotDBSchema.IotDBRowValue;
import com.fuzzy.iotdb.IotDBSchema.IotDBTables;
import com.fuzzy.iotdb.IotDBVisitor;
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.ast.IotDBCastOperation.CastType;
import com.fuzzy.iotdb.ast.IotDBUnaryPostfixOperation.UnaryPostfixOperator;
import com.fuzzy.iotdb.feedback.IotDBQuerySynthesisFeedbackManager;
import com.fuzzy.iotdb.gen.IotDBExpressionGenerator;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class IotDBPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<IotDBGlobalState, IotDBRowValue, IotDBExpression, SQLConnection> {

    private List<IotDBExpression> fetchColumns;
    private List<IotDBColumn> columns;

    public IotDBPivotedQuerySynthesisOracle(IotDBGlobalState globalState) throws SQLException {
        super(globalState);
        IotDBErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        IotDBSchema schema = globalState.getSchema();
        IotDBTables randomFromTables = schema.getOneRandomTableNonEmptyTables();

        IotDBSelect selectStatement = new IotDBSelect();
        pivotRow = randomFromTables.getRandomRowValue(globalState);

        selectStatement.setFromList(randomFromTables.getTables().stream().map(IotDBTableReference::new)
                .collect(Collectors.toList()));

        // 时间戳字段设置表信息(get(0)) -> 该数据库下任意表均有该字段
        columns = randomFromTables.getColumns();
        fetchColumns = columns.stream().map(c -> new IotDBColumnReference(c, null)).collect(Collectors.toList());

        // TODO 将time字段移除运算条件生成列
//        IotDBColumn timeColumn = new IotDBColumn(IotDBValueStateConstant.TIME_COLUMN.getValue(), false,
//                IotDBDataType.INT64);
//        timeColumn.setTable(randomFromTables.getTables().get(0));
//        columns.add(timeColumn);
        // 划分cast和普通Column
        int splitCastColumnIndex = globalState.getRandomly().getInteger(0, fetchColumns.size());
        // cast Columns
        selectStatement.setCastColumns(fetchColumns.subList(0, splitCastColumnIndex).stream().map(columnRef ->
                new IotDBCastOperation(columnRef, CastType.getRandom(
                        ((IotDBColumnReference) columnRef).getColumn().getType()))).collect(Collectors.toList()));
        // fetch Columns
        selectStatement.setFetchColumns(fetchColumns.subList(splitCastColumnIndex, fetchColumns.size()));
        IotDBExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        // TODO 仅当执行聚合函数时才支持group by
        // 且limit语句存在时不支持group by
//        List<IotDBExpression> groupByClause = generateGroupByClause(columns, pivotRow);
//        selectStatement.setGroupByExpressions(groupByClause);
        IotDBExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            IotDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<IotDBExpression> orderBy = new IotDBExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);

        // between
        errors.add("Msg: 301: The Type of three subExpression should be all Numeric or Text");
        errors.add("Unsupported constant operand: NULL");
        errors.add("Unsupported Type");
        errors.add("The Type of three subExpression should be all Numeric or Text");
        return new SQLQueryAdapter(IotDBVisitor.asString(selectStatement), errors);
    }

    @Override
    protected void setSequenceRegenerateProbabilityToMax(String sequence) {
        IotDBQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(sequence);
    }

    @Override
    protected void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType) {
        switch (queryType) {
            case error:
                IotDBQuerySynthesisFeedbackManager.incrementErrorQueryCount();
                break;
            case invalid:
                IotDBQuerySynthesisFeedbackManager.incrementInvalidQueryCount();
                break;
            case success:
                IotDBQuerySynthesisFeedbackManager.incrementSuccessQueryCount();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Invalid QueryExecutionType: %s", queryType));
        }
    }

    @Override
    protected String asSequenceString(IotDBExpression expr) {
        return IotDBVisitor.asString(expr, true);
    }

    private List<IotDBExpression> generateGroupByClause(List<IotDBColumn> columns, IotDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> IotDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private IotDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return IotDBConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private IotDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return IotDBConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private IotDBExpression generateRectifiedExpression(List<IotDBColumn> columns, IotDBRowValue rw) {
        IotDBExpression result = null;
        int regenerateCounter = 0;
        boolean reGenerateExpr;
        do {
            try {
                reGenerateExpr = false;
                IotDBExpression expression = new IotDBExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                        .generateExpression();
                // 根据抽象语法树（AST）解释器的计算结果, 修改谓词, 使其查询结果必定包含pivot row
                // is null生成全部去掉, true or false生成修改Not取反
                // where 条件后按理来说只能生成Boolean型数据，非Boolean型数据全部重新生成
                IotDBConstant expectedValue = expression.getExpectedValue();
                if (expectedValue.isNull()) {
                    result = new IotDBUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
                } else if (expectedValue.asBooleanNotNull()) {
                    result = expression;
                } else {
                    result = IotDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(expression);
                }

                String sequence = IotDBVisitor.asString(result, true);
                if (globalState.getOptions().useSyntaxSequence()
                        && IotDBQuerySynthesisFeedbackManager.isRegenerateSequence(sequence)) {
                    regenerateCounter++;
                    if (regenerateCounter >= IotDBQuerySynthesisFeedbackManager.MAX_REGENERATE_COUNT_PER_ROUND) {
                        IotDBQuerySynthesisFeedbackManager.incrementExpressionDepth(
                                globalState.getOptions().getMaxExpressionDepth());
                        regenerateCounter = 0;
                    }
                    throw new ReGenerateExpressionException(String.format("该语法节点序列需重新生成:%s", sequence));
                }

                // 更新概率表
                IotDBQuerySynthesisFeedbackManager.addSequenceRegenerateProbability(sequence);
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
        // IotDB不支持子查询，将PQS子查询结构进行改造
        StringBuilder sb = new StringBuilder();
        String[] querySegment = query.getUnterminatedQueryString().split("WHERE");
        assert querySegment.length == 2;

        sb.append(querySegment[0]);
        sb.append("WHERE ");
        int i = 0;
        for (IotDBColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else if (pivotRow.getValues().get(c).isDouble()) {
                sb.append(" >= ")
                        .append(BigDecimal.valueOf(pivotRow.getValues().get(c).getDouble())
                                .subtract(BigDecimal.valueOf(Math.pow(10, -IotDBConstant.IotDBDoubleConstant.scale)))
                                .setScale(IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString())
                        .append(" AND ")
                        .append(c.getName())
                        .append(" <= ")
                        .append(BigDecimal.valueOf(pivotRow.getValues().get(c).getDouble())
                                .add(BigDecimal.valueOf(Math.pow(10, -IotDBConstant.IotDBDoubleConstant.scale)))
                                .setScale(IotDBConstant.IotDBDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString());
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
    protected String getExpectedValues(IotDBExpression expr) {
        return IotDBVisitor.asExpectedValues(expr);
    }
}
