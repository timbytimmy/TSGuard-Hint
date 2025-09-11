package com.fuzzy.prometheus.gen;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.prometheus.PrometheusGlobalState;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import com.fuzzy.prometheus.apiEntry.PrometheusInsertParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import com.fuzzy.prometheus.constant.PrometheusLabelConstant;

import java.util.HashMap;
import java.util.Map;

public class PrometheusDatabaseGenerator {
    private final String databaseName;
    private final Randomly r;
    private final PrometheusGlobalState globalState;

    public PrometheusDatabaseGenerator(PrometheusGlobalState globalState, String databaseName) {
        this.databaseName = databaseName;
        this.r = globalState.getRandomly();
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(PrometheusGlobalState globalState, String databaseName) {
        return new PrometheusDatabaseGenerator(globalState, databaseName).create();
    }

    private SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();

        // 创建database -> 创建table、column标签均为databaseInit的Counter, 进行数据插入
        String databaseInitLabel = PrometheusLabelConstant.DATABASE_INIT.getLabel();
        CollectorAttribute attribute = new CollectorAttribute();
        attribute.setDataType(PrometheusDataType.COUNTER);
        attribute.setMetricName(String.format("%s_%s", databaseName, databaseInitLabel));
        attribute.setHelp(String.format("%s.%s.%s", databaseInitLabel, databaseInitLabel, databaseInitLabel));
        attribute.setDatabaseName(databaseName);
        attribute.setTableName(databaseInitLabel);
        attribute.setIntValue(1);

        Map<String, CollectorAttribute> collectorMap = new HashMap<>();
        collectorMap.put(attribute.getMetricName(), attribute);
        PrometheusInsertParam insertParam = new PrometheusInsertParam();
        insertParam.setType(PrometheusRequestType.push_data);
        insertParam.setCollectorList(collectorMap);
        return new SQLQueryAdapter(JSONObject.toJSONString(insertParam), errors, true);
    }

}
