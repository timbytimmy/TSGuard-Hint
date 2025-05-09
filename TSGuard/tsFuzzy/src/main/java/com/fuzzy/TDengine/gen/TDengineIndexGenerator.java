package com.fuzzy.TDengine.gen;


import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDengineIndexGenerator {
    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private final TDengineGlobalState globalState;

    public TDengineIndexGenerator(TDengineGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(TDengineGlobalState globalState, String tableName) {
        return new TDengineIndexGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        // CREATE INDEX test_index ON super_t1 (group_id);
        String indexName = IndexName.group_id.name();
        sb.append("CREATE INDEX ").append(tableName).append("_").append(indexName).append("_index").append(" ON ")
                .append(tableName).append(" (").append(indexName).append(");");
        return new SQLQueryAdapter(sb.toString(), new ExpectedErrors(), false);
    }

    private enum IndexName {
        location, group_id
    }
}
