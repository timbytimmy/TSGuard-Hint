package com.fuzzy.influxdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.influxdb.InfluxDBErrors;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;
import com.fuzzy.influxdb.InfluxDBVisitor;
import com.fuzzy.influxdb.resultSet.InfluxDBSeries;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class InfluxDBUpdateGenerator {

    private final InfluxDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final InfluxDBGlobalState globalState;

    public InfluxDBUpdateGenerator(InfluxDBGlobalState globalState, InfluxDBTable table) {
        this.globalState = globalState;
        this.table = table;
    }

    public static SQLQueryAdapter replaceRow(InfluxDBGlobalState globalState) throws SQLException {
        InfluxDBTable table = globalState.getSchema().getRandomTable();
        return replaceRow(globalState, table);
    }

    private static SQLQueryAdapter replaceRow(InfluxDBGlobalState globalState, InfluxDBTable table) throws SQLException {
        return new InfluxDBUpdateGenerator(globalState, table).generateReplace();
    }

    private SQLQueryAdapter generateReplace() throws SQLException {
        // 拉取原有数据值, 随机选取若干行, 替换Field后重新插入
        InfluxDBSeries influxDBSeries = table.getRowValues(globalState.getConnection(),
                globalState.getOptions().getPrecision());
        InfluxDBExpressionGenerator gen = new InfluxDBExpressionGenerator(globalState);
        List<InfluxDBColumn> fieldColumns = table.getFieldColumns();
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }

        for (int row = 0; row < nrRows; row++) {
            String randomSeries = table.getRandomSeries();
            sb.append(randomSeries)
                    .append(" ");

            for (int c = 0; c < fieldColumns.size(); c++) {
                sb.append(fieldColumns.get(c).getName())
                        .append("=")
                        .append(InfluxDBVisitor.asString(
                                gen.generateConstantForInfluxDBDataType(fieldColumns.get(c).getType())))
                        .append(",");
            }

            // 选择随机序列中现存时间戳进行替换field
            Map<String, String> tagMap = seriesToTagMap(randomSeries);
            sb.deleteCharAt(sb.length() - 1)
                    .append(" ")
                    .append(Randomly.fromList(influxDBSeries.getTimestampForSeries(tagMap)))
                    .append("\n");
        }
        InfluxDBErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.deleteCharAt(sb.length() - 1).toString(), errors);
    }

    private Map<String, String> seriesToTagMap(String randomSeries) {
        Map<String, String> tagMap = new HashMap<>();
        String[] parts = randomSeries.split("(?<!\\\\),");
        for (int i = 1; i < parts.length; i++) {
            String[] tag = parts[i].split("(?<!\\\\)=");
            assert tag.length == 2;
            tagMap.put(tag[0], tag[1]);
        }
        return tagMap;
    }

}
