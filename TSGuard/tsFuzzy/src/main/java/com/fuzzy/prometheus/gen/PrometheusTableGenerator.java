package com.fuzzy.prometheus.gen;

import com.fuzzy.Randomly;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import com.fuzzy.prometheus.apiEntry.PrometheusInsertParam;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrometheusTableGenerator {
    private final String tableName;
    private final Randomly r;
    private final List<String> columns = new ArrayList<>();
    private final PrometheusSchema schema;
    private final PrometheusGlobalState globalState;

    public PrometheusTableGenerator(PrometheusGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.r = globalState.getRandomly();
        this.schema = globalState.getSchema();
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(PrometheusGlobalState globalState, String tableName) {
        return new PrometheusTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        // 创建Table -> 创建若干column
        ExpectedErrors errors = new ExpectedErrors();

        Map<String, CollectorAttribute> collectorMap = new HashMap<>();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            String columnName = genColumn(i);
            CollectorAttribute attribute = new CollectorAttribute();
            attribute.setDataType(PrometheusDataType.getRandom(globalState));
            attribute.setMetricName(globalState.getDatabaseName());
            attribute.setHelp(String.format("%s.%s.%s", globalState.getDatabaseName(), tableName, columnName));
//            attribute.setDatabaseName(globalState.getDatabaseName());
            attribute.setTableName(tableName);
            attribute.setTimeSeriesName(columnName);
            attribute.randomInitValue(r);
            collectorMap.put(attribute.getMetricName(), attribute);
        }

        PrometheusInsertParam insertParam = new PrometheusInsertParam();
        insertParam.setCollectorList(collectorMap);
        return new SQLQueryAdapter(insertParam.genPrometheusQueryParam(), errors, true);
    }

    private String genColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        return columnName;
    }

}
