package com.fuzzy.griddb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.griddb.GridDBErrors;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema;
import com.fuzzy.griddb.GridDBSchema.GridDBTable;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class GridDBIndexGenerator {

    private final GridDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final GridDBGlobalState globalState;

    public GridDBIndexGenerator(GridDBGlobalState globalState, GridDBTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter createIndex(GridDBGlobalState globalState) throws SQLException {
        GridDBTable table = globalState.getSchema().getRandomTable();
        return createIndex(globalState, table);
    }

    public static SQLQueryAdapter createIndex(GridDBGlobalState globalState, GridDBTable table) throws SQLException {
        return new GridDBIndexGenerator(globalState, table).generateIndex();
    }

    private SQLQueryAdapter generateIndex() {
        sb.append("CREATE INDEX ").append("IF NOT EXISTS ");
        List<GridDBSchema.GridDBColumn> columns = table.getColumns().stream().filter(
                        t -> !t.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName()))
                .collect(Collectors.toList());
        GridDBSchema.GridDBColumn column = Randomly.fromList(columns);
        String indexName = "index_" + table.getName() + "_" + column.getName();
        sb.append(indexName).append(" ");
        sb.append("ON ");
        sb.append(table.getName());
        sb.append("(").append(column.getName()).append(")");
        GridDBErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false);
    }
}
