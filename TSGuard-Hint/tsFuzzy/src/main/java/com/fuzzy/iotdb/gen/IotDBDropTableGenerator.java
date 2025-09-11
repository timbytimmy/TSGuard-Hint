package com.fuzzy.iotdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema;

public final class IotDBDropTableGenerator {

    private IotDBDropTableGenerator() {
    }

    public static SQLQueryAdapter dropRandomTable(IotDBGlobalState globalState) {
        IotDBSchema.IotDBTable randomTable = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("DROP TIMESERIES ");
        sb.append(globalState.getDatabaseName())
                .append(".").append(randomTable.getName())
                .append(".").append(Randomly.fromList(randomTable.getColumns()).getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"), true);
    }

}
