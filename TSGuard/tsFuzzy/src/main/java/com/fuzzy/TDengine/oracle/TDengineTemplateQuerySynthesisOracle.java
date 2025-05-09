package com.fuzzy.TDengine.oracle;


import com.fuzzy.IgnoreMeException;
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
import com.fuzzy.TDengine.ast.TDengineColumnReference;
import com.fuzzy.TDengine.ast.TDengineConstant;
import com.fuzzy.TDengine.ast.TDengineExpression;
import com.fuzzy.TDengine.gen.TDengineTimeSeriesConstantGenerator;
import com.fuzzy.TDengine.gen.TDengineTimeSeriesParameterGenerator;
import com.fuzzy.TDengine.resultSet.TDengineResultSet;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.TDengine.tsaf.template.TDengineTemplateValues;
import com.fuzzy.TDengine.tsaf.template.TDengineTimeSeriesTemplate;
import com.fuzzy.common.oracle.TemplateQuerySynthesisBase;
import com.fuzzy.common.query.Query;
import com.fuzzy.common.query.SQLQueryAdapter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TDengineTemplateQuerySynthesisOracle
        extends TemplateQuerySynthesisBase<TDengineGlobalState, TDengineRowValue, TDengineExpression, SQLConnection> {

    private List<TDengineColumn> columns;
    private TDengineTable table;
    private final TDengineTimeSeriesConstantGenerator gen;
    private TDengineTimeSeriesParameterGenerator parameterGenerator;

    public TDengineTemplateQuerySynthesisOracle(TDengineGlobalState globalState) throws SQLException {
        super(globalState);
        gen = new TDengineTimeSeriesConstantGenerator(globalState);
        TDengineErrors.addExpressionErrors(errors);
    }

    @Override
    public Query<SQLConnection> getTimeSeriesQuery() throws SQLException {
        TDengineSchema schema = globalState.getSchema();
        TDengineTables randomFromTables = schema.getRandomTableNonEmptyTables();
        List<TDengineTable> tables = randomFromTables.getTables();
        assert tables.size() == 1;
        table = tables.get(0);
        columns = randomFromTables.getColumns();
        // 随机选择测试模板
        template = Randomly.fromOptions(TDengineTimeSeriesTemplate.values());
        parameterGenerator = new TDengineTimeSeriesParameterGenerator(table, globalState);
        String selectClause = generateSelectClause(template);
        timeSeriesPredicates.add(template.getValues());
        return new SQLQueryAdapter(selectClause, errors);
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

    private String generateSelectClause(TDengineTimeSeriesTemplate template) {
        StringBuilder sb = new StringBuilder();
        sb.append(template.getTemplate().getTemplateString());

        // 依据查询模板选择赋值
        template.genRandomTemplateValues(table, globalState);

        // 替换模板占位符
        for (int i = 0; i < template.getValues().size(); i++) {
            Object value = template.getValues().get(i);
            int index = sb.indexOf("?");
            if (index != -1) sb.replace(index, index + 1, value.toString());
        }
        return sb.toString();
    }

    @Override
    protected String getExpectedValues(TDengineTemplateValues timeSeriesPredicate) {
        // TODO 依据模板获取预期值
        return TDengineVisitor.asExpectedValues(gen.generateConstantByTimestamp((long) timeSeriesPredicate.get(0)));
    }

    @Override
    public boolean verifyResults(Query<SQLConnection> query) throws Exception {
        try (TDengineResultSet result = (TDengineResultSet) query.executeAndGet(globalState)) {
            if (result == null) {
                log.info("无效查询.. query:{};", query.getQueryString());
                throw new IgnoreMeException();
            }

            // 验证器 -> 依据template类型验证
            List<TDengineSchema.TDengineColumn> verifyColumns = columns.stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());
            return template.verifyResults(result, verifyColumns, gen, parameterGenerator);
        }
    }
}
