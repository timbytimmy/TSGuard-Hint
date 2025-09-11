package com.fuzzy.influxdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.influxdb.InfluxDBErrors;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;
import com.fuzzy.influxdb.InfluxDBVisitor;

public class InfluxDBDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final InfluxDBGlobalState globalState;

    public InfluxDBDeleteGenerator(InfluxDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(InfluxDBGlobalState globalState) {
        return new InfluxDBDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        InfluxDBExpressionGenerator gen = new InfluxDBExpressionGenerator(globalState).setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE");
        boolean hasFromClause = false;
        if (Randomly.getBoolean()) {
            hasFromClause = true;
            sb.append(" FROM ");
            sb.append(randomTable.getFullName());
        }
        // where 和 from 必须包含至少一个
        if (Randomly.getBoolean() || !hasFromClause) {
            sb.append(" WHERE ");
            sb.append(InfluxDBVisitor.asString(gen.generateExpression()));
            InfluxDBErrors.addExpressionErrors(errors);
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
