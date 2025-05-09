package com.fuzzy.iotdb.gen;


import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.iotdb.IotDBErrors;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable;

public class IotDBDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final IotDBGlobalState globalState;

    public IotDBDeleteGenerator(IotDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(IotDBGlobalState globalState) {
        return new IotDBDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        IotDBTable randomTable = globalState.getSchema().getRandomTable();
        IotDBExpressionGenerator gen = new IotDBExpressionGenerator(globalState).setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE FROM ");
        sb.append(randomTable.getName())
                .append(" WHERE ");
        // TODO For delete statement, where clause can only contain atomic expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
        sb.append("time >= ");
        sb.append(globalState.getRandomTimestamp());
        // sb.append(IotDBVisitor.asString(gen.generateExpression()));
        IotDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false);
    }

}
