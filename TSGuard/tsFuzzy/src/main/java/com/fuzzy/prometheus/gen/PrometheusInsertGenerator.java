package com.fuzzy.prometheus.gen;


import com.alibaba.fastjson.JSONObject;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusTable;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import com.fuzzy.prometheus.apiEntry.PrometheusInsertParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import io.prometheus.client.CollectorRegistry;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class PrometheusInsertGenerator {

    private final PrometheusTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final PrometheusGlobalState globalState;

    public PrometheusInsertGenerator(PrometheusGlobalState globalState, PrometheusTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter insertRow(PrometheusGlobalState globalState) throws SQLException {
        PrometheusTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(PrometheusGlobalState globalState, PrometheusTable table) throws SQLException {
        return new PrometheusInsertGenerator(globalState, table).generateInsert();
    }

    private SQLQueryAdapter generateInsert() {
        // 仅支持插入最新数据进行覆盖, 将以历史数据作为行数据
        Map<String, CollectorAttribute> collectorMap = new HashMap<>();

        table.getRandomNonEmptyColumnSubset().forEach(column -> {
            CollectorAttribute attribute = new CollectorAttribute();
            attribute.setDataType(column.getType());
            attribute.setMetricName(column.getName());
            attribute.setHelp(String.format("%s.%s.%s", table.getDatabaseName(), table.getName(), column.getName()));
            attribute.setDatabaseName(table.getDatabaseName());
            attribute.setTableName(table.getName());
            attribute.randomInitValue(globalState.getRandomly());
            collectorMap.put(column.getName(), attribute);
        });

        PrometheusInsertParam insertParam = new PrometheusInsertParam();
        insertParam.setType(PrometheusRequestType.push_data);
        insertParam.setCollectorList(collectorMap);
        return new SQLQueryAdapter(JSONObject.toJSONString(insertParam), errors);
    }

}
