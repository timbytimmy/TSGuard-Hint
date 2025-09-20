package com.fuzzy.common.oracle;


import cn.hutool.core.collection.ConcurrentHashSet;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.GlobalState;
import com.fuzzy.IgnoreMeException;
import com.fuzzy.TSFuzzyDBConnection;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.QueryExecutionStatistical;
import com.fuzzy.common.schema.AbstractRowValue;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class PivotedQuerySynthesisBase<S extends GlobalState<?, ?, C>, R extends AbstractRowValue<?, ?, ?>, E, C extends TSFuzzyDBConnection>
        implements TestOracle<S> {

    protected final ExpectedErrors errors = new ExpectedErrors();

    /**
     * The predicates used in WHERE and JOIN clauses, which yield TRUE for the pivot row.
     */
    protected final List<E> rectifiedPredicates = new ArrayList<>();
    protected static final ConcurrentHashSet<String> pivotRowAbstractPredicates = new ConcurrentHashSet<>();

    protected final S globalState;
    protected R pivotRow;

    protected PivotedQuerySynthesisBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public final void check() throws Exception {
        // TODO pqs得到的查询请求需要做多层限制？大部分未命中的请求是因为各数据库不支持相关功能？
        rectifiedPredicates.clear();
        Query<C> pivotRowQuery = getRectifiedQuery();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(pivotRowQuery.getQueryString());
        }
        Query<C> isContainedQuery = getContainmentCheckQuery(pivotRowQuery);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(isContainedQuery.getQueryString());
        }
        globalState.getState().getLocalState().log(isContainedQuery.getQueryString());
        // 语法正确查询 -> 测逻辑异常（能够执行, 则判断PivotRow是否被嵌套查询包含）
        boolean pivotRowIsContained = containsRows(isContainedQuery);
        if (!pivotRowIsContained) {
            reportMissingPivotRow(pivotRowQuery);
        } else {
            // statistical
            incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.success);
            log.info("有效查询: {}", isContainedQuery.getQueryString());
        }
    }

    /**
     * Checks whether the result set contains at least a single row.
     *
     * @param query the query for which to check whether its result set contains any rows
     * @return true if at least one row is contained, false otherwise
     * @throws Exception if the query unexpectedly fails
     */
    private boolean containsRows(Query<C> query) throws Exception {
        try (DBValResultSet result = query.executeAndGet(globalState)) {
            if (result == null) {
                globalState.getLogger().writeSyntaxErrorQuery(String.format("无效查询: %s;",
                        query.getQueryString()));
                // 将无效查询序列重生成概率设为10
                String abstractExpr = asSequenceString(rectifiedPredicates.get(0));
                setSequenceRegenerateProbabilityToMax(abstractExpr);
                // statistical
                incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.invalid);
                throw new IgnoreMeException();
            }
            return result.hasNext();
        }
    }

    protected void reportMissingPivotRow(Query<?> query) {
        // statistical
        incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType.error);

        globalState.getState().getLocalState().log("-- pivot row values:");
        String expectedPivotRowString = pivotRow.asStringGroupedByTables();
        globalState.getState().getLocalState().log(expectedPivotRowString);

        StringBuilder sb = new StringBuilder();
        if (!rectifiedPredicates.isEmpty()) {
            sb.append("--\n-- rectified predicates and their expected values:\n");
            for (E rectifiedPredicate : rectifiedPredicates) {
                sb.append("--");
                sb.append(getExpectedValues(rectifiedPredicate).replace("\n", "\n-- "));

//                TDengineConstant expectedValue = ((TDengineExpression) rectifiedPredicate).getExpectedValue();

                // 表达式抽象结构
                String abstractExpr = asSequenceString(rectifiedPredicate);

                pivotRowAbstractPredicates.forEach(pivotRow -> {
                    if (pivotRow.contains(abstractExpr)) {
                        // 探索空间：表达式递归深度逐级递增
                        // => 从小序列搜集到大序列, 但凡存在包含关系的pivotRowAbstractPredicates均忽略重复报错
                        System.out.println("序列报错重复");
                    }
                });
                sb.append("--\n-- rectified predicates abstract expression:\n")
                        .append(abstractExpr)
                        .append("\n--");
                pivotRowAbstractPredicates.add(abstractExpr);
                // 将出错序列重生成概率设为10
                setSequenceRegenerateProbabilityToMax(abstractExpr);
            }
            sb.append("\n");
        }
        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    /**
     * Gets a query that checks whether the pivot row is contained in the result. If the pivot row is contained, the
     * query will fetch at least one row. If the pivot row is not contained, no rows will be fetched. This corresponds
     * to step 7 described in the PQS paper.
     *
     * @param pivotRowQuery the query that is guaranteed to fetch the pivot row, potentially among other rows
     * @return a query that checks whether the pivot row is contained in pivotRowQuery
     * @throws Exception if an unexpected error occurs
     */
    protected abstract Query<C> getContainmentCheckQuery(Query<?> pivotRowQuery) throws Exception;

    /**
     * Obtains a rectified query (i.e., a query that is guaranteed to fetch the pivot row. This corresponds to steps 2-5
     * of the PQS paper.
     *
     * @return the rectified query
     * @throws Exception if an unexpected error occurs
     */
    protected abstract Query<C> getRectifiedQuery() throws Exception;

    protected abstract void setSequenceRegenerateProbabilityToMax(String sequence);

    protected abstract void incrementQueryExecutionCounter(QueryExecutionStatistical.QueryExecutionType queryType);

    protected abstract String asSequenceString(E expr);

    /**
     * Prints the value to which the expression is expected to evaluate, and then recursively prints the subexpressions'
     * expected values.
     *
     * @param expr the expression whose expected value should be printed
     * @return a string representing the expected value of the expression and its subexpressions
     */
    protected abstract String getExpectedValues(E expr);

}
