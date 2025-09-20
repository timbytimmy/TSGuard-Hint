package com.fuzzy.common.oracle;


import com.fuzzy.GlobalState;
import com.fuzzy.TDengine.tsaf.template.TDengineTemplateValues;
import com.fuzzy.TDengine.tsaf.template.TDengineTimeSeriesTemplate;
import com.fuzzy.TSFuzzyDBConnection;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.schema.AbstractRowValue;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class TemplateQuerySynthesisBase<S extends GlobalState<?, ?, C>, R extends AbstractRowValue<?, ?, ?>, E, C extends TSFuzzyDBConnection>
        implements TestOracle<S> {

    protected final ExpectedErrors errors = new ExpectedErrors();

    /**
     * The predicates used in WHERE and JOIN clauses, which yield TRUE for the pivot row.
     */
    protected final List<TDengineTemplateValues> timeSeriesPredicates = new ArrayList<>();

    /**
     * The generalization of a pivot row, as explained in the "Checking arbitrary expressions" paragraph of the PQS
     * paper.
     */
    protected List<E> pivotRowExpression = new ArrayList<>();
    protected final S globalState;
    protected TDengineTimeSeriesTemplate template;

    protected TemplateQuerySynthesisBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public final void check() throws Exception {
        timeSeriesPredicates.clear();
        Query<C> timeSeriesQuery = getTimeSeriesQuery();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(timeSeriesQuery.getQueryString());
        }
        // 执行查询 -> 本地依据template生成结果比对
        boolean resultIsMatch = verifyResults(timeSeriesQuery);
        if (!resultIsMatch) {
            reportErrorQueryResult(timeSeriesQuery);
        } else {
            log.info("有效查询: {}", timeSeriesQuery.getQueryString());
        }
    }

    /**
     * Checks whether the result set contains at least a single row.
     *
     * @param query the query for which to check whether its result set contains any rows
     * @return true if at least one row is contained, false otherwise
     * @throws Exception if the query unexpectedly fails
     */
    protected abstract boolean verifyResults(Query<C> query) throws Exception;

    protected void reportErrorQueryResult(Query<?> query) {
        globalState.getState().getLocalState().log("-- error query results:");
        globalState.getState().getLocalState().log(template.toString());

//        StringBuilder sb = new StringBuilder();
//        if (!timeSeriesPredicates.isEmpty()) {
//            sb.append("--\n-- rectified predicates and their expected values:\n");
//            for (TDengineTemplateValues timeSeriesPredicate : timeSeriesPredicates) {
//                sb.append("--");
//                sb.append(getExpectedValues(timeSeriesPredicate).replace("\n", "\n-- "));
//            }
//            sb.append("\n");
//        }
//        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    /**
     * Obtains a TimeSeries query (i.e., a query that is guaranteed to fetch the pivot row. This corresponds to steps 2-5
     * of the PQS paper.
     *
     * @return the rectified query
     * @throws Exception if an unexpected error occurs
     */
    protected abstract Query<C> getTimeSeriesQuery() throws Exception;

    /**
     * Prints the value to which the expression is expected to evaluate, and then recursively prints the subexpressions'
     * expected values.
     *
     * @param timeSeriesPredicate the expression whose expected value should be printed
     * @return a string representing the expected value of the expression and its subexpressions
     */
    protected abstract String getExpectedValues(TDengineTemplateValues timeSeriesPredicate);

}
