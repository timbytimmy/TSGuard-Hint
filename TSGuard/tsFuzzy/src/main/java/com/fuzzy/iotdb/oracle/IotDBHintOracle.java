package com.fuzzy.iotdb.oracle;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.iotdb.*;
import com.fuzzy.iotdb.IotDBSchema.*;
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.feedback.IotDBQuerySynthesisFeedbackManager;
import com.fuzzy.iotdb.gen.IotDBExpressionGenerator;
import com.fuzzy.iotdb.gen.IotDBTimeExpressionGenerator;
import com.fuzzy.iotdb.hint.IotDBHintBuilder;
import com.fuzzy.iotdb.hint.IotDBResultComparator;
import com.fuzzy.iotdb.hint.IotDBResultNormalizer;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.fuzzy.iotdb.hint.IotDBHintStats.*;

@Slf4j
public class IotDBHintOracle
        extends com.fuzzy.common.oracle.TimeSeriesAlgebraFrameworkBase<IotDBGlobalState, IotDBRowValue, IotDBExpression, SQLConnection>
        implements TestOracle<IotDBGlobalState> {

    private IotDBSelect selectStatement;
    private IotDBTable table;
    private List<IotDBColumn> columns;

    public IotDBHintOracle(IotDBGlobalState globalState) {
        super(globalState);
        IotDBErrors.addExpressionErrors(errors);
    }

    // ------------ Oracle entry point ------------
    @Override
    public void check() throws Exception {
        globalState.getLogger().writeCurrent("-- HINT CHECK START --");

        SQLQueryAdapter baseline = (SQLQueryAdapter) getTimeSeriesQuery();
        String baselineSql = baseline.getQueryString();

        // Force-write the SQL you’re about to run
        globalState.getLogger().writeCurrent(baselineSql + " ; /* baseline */");

        // IMPORTANT: execute via SQLQueryAdapter.executeAndGet so it also logs internally
        try (DBValResultSet baseAny = new SQLQueryAdapter(baselineSql).executeAndGet(globalState)) {
            IotDBResultSet rsBase = (IotDBResultSet) baseAny;
            var nBase = IotDBResultNormalizer.normalize(rsBase);

            var variants = IotDBHintBuilder.safeVariants(baselineSql);
            for (String hintedSql : variants) {
                if (hintedSql.equals(baselineSql)) continue;

                // Force-write the hinted SQL too
                globalState.getLogger().writeCurrent(hintedSql + " ; /* hinted */");

                try (DBValResultSet hintAny = new SQLQueryAdapter(hintedSql).executeAndGet(globalState)) {
                    IotDBResultSet rsHint = (IotDBResultSet) hintAny;
                    var nHint = IotDBResultNormalizer.normalize(rsHint);

                    PAIRS.incrementAndGet();
                    var diff = IotDBResultComparator.compare(nBase, nHint);
                    if (diff != null) {
                        MISMATCHES.incrementAndGet();
                        incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.error);
                        globalState.getLogger().writeSyntaxErrorQuery(
                                "[HINT_MISMATCH]\nBaseline:\n" + baselineSql + "\n\nHinted:\n" + hintedSql + "\n\n" + diff);
                        throw new AssertionError("Hint-based mismatch detected");
                    } else {
                        MATCHES.incrementAndGet();
                        incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.success);
                    }
                }
            }
        }
    }



    // baseline SELECT
    @Override
    public Query<SQLConnection> getTimeSeriesQuery() {
        selectStatement = new IotDBSelect();
        IotDBSchema schema = globalState.getSchema();
        IotDBTables nonEmpty = schema.getOneRandomTableNonEmptyTables();
        table = Randomly.fromList(nonEmpty.getTables());
        columns = table.getColumns();

        selectStatement.setFromList(Collections.singletonList(new IotDBTableReference(table)));

        selectStatement.setQueryType(com.fuzzy.common.tsaf.QueryType.BASE_QUERY);

        List<IotDBExpression> fetch = columns.stream()
                .map(c -> new IotDBColumnReference(c, null))
                .collect(Collectors.toList());
        selectStatement.setFetchColumns(fetch);

        // WHERE predicate (boolean)
        IotDBExpression predicate = generatePredicate(columns);
        selectStatement.setWhereClause(predicate);

        return new SQLQueryAdapter(IotDBVisitor.asString(selectStatement), errors);
    }

    private IotDBExpression generatePredicate(List<IotDBColumn> columns) {
        IotDBExpression dataPred = new IotDBExpressionGenerator(globalState).setColumns(columns).generateExpression();
        if (!dataPred.getExpectedValue().isBoolean()) {
            dataPred = IotDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(dataPred);
        }

        IotDBTimeExpressionGenerator timeGen = new IotDBTimeExpressionGenerator(globalState);
        IotDBExpression timeExpr = timeGen.generateExpression(); // something like time >= ... etc.
        // If time expressions return a boolean, AND them; otherwise, skip
        IotDBExpression result = dataPred;
        if (!ObjectUtils.isEmpty(timeExpr) && timeExpr.getExpectedValue().isBoolean()) {
            result = new IotDBBinaryLogicalOperation(
                    dataPred, timeExpr, IotDBBinaryLogicalOperation.IotDBBinaryLogicalOperator.AND);
        }
        return result;
    }

    // Base-class hooks
    @Override
    protected void setSequenceRegenerateProbabilityToMax(String seq) {
        IotDBQuerySynthesisFeedbackManager.setSequenceRegenerateProbabilityToMax(seq);
    }

    @Override
    protected void incrementQueryExecutionCounter(
            QueryExecutionStatistical.QueryExecutionType t) {
        switch (t) {
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
                throw new UnsupportedOperationException();
        }
    }


    @Override
    protected TimeSeriesConstraint genColumnConstraint(IotDBExpression expr) {
        return IotDBVisitor.asConstraint(
                globalState.getDatabaseName(),
                table.getName(),
                expr,
                TableToNullValuesManager.getNullValues(globalState.getDatabaseName(), table.getName()));
    }

    @Override
    protected Map<Long, List<BigDecimal>> getExpectedValues(IotDBExpression expression) {
        return Collections.emptyMap();
    }

    @Override
    protected boolean verifyResultSet(Map<Long, List<BigDecimal>> expected, DBValResultSet result) {
        return true; // comparison happens in check()
    }
}
