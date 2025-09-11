package com.fuzzy.influxdb.gen;


import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.influxdb.InfluxDBGlobalState;

public final class InfluxDBDropMeasurementGenerator {

    private InfluxDBDropMeasurementGenerator() {
    }

    public static SQLQueryAdapter generate(InfluxDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("q=DROP MEASUREMENT ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"), true);
    }

}
