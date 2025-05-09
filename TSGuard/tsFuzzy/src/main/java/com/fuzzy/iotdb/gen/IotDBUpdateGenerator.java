package com.fuzzy.iotdb.gen;


import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.iotdb.IotDBErrors;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema.IotDBColumn;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable;
import com.fuzzy.iotdb.IotDBVisitor;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class IotDBUpdateGenerator {

    private final IotDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final IotDBGlobalState globalState;
    private final List<Long> timestamps = new ArrayList<>();

    public IotDBUpdateGenerator(IotDBGlobalState globalState, IotDBTable table) throws SQLException {
        this.globalState = globalState;
        this.table = table;
        String selectTimeSql = String.format("SELECT * FROM %s", table.getDatabaseName());
        try (TSFuzzyStatement s = globalState.getConnection().createStatement()) {
            try (IotDBResultSet iotDBResultSet = (IotDBResultSet)
                    s.executeQuery(selectTimeSql)) {
                while (iotDBResultSet.hasNext()) {
                    timestamps.add(iotDBResultSet.getCurrentValue().getTimestamp());
                }
            } catch (Exception e) {
                throw new SQLException(e.getMessage());
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        // 防止更新操作比插入先执行导致空值问题
        if (timestamps.isEmpty()) timestamps.add(globalState.getRandomTimestamp());
    }

    public static SQLQueryAdapter replaceRow(IotDBGlobalState globalState) throws SQLException {
        IotDBTable table = globalState.getSchema().getRandomTable();
        return replaceRow(globalState, table);
    }

    private static SQLQueryAdapter replaceRow(IotDBGlobalState globalState, IotDBTable table) throws SQLException {
        return new IotDBUpdateGenerator(globalState, table).generateReplace();
    }

    private SQLQueryAdapter generateReplace() throws SQLException {
        // 拉取原有数据值, 随机选取若干行, 替换value后重新插入
        sb.append("INSERT INTO ");
        sb.append(table.getDatabaseName());
        List<IotDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(timestamp, ");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        IotDBExpressionGenerator gen = new IotDBExpressionGenerator(globalState);
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            sb.append("(");
            // timestamp -> 从数据中获取
            sb.append(Randomly.fromList(timestamps))
                    .append(", ");

            for (int c = 0; c < columns.size(); c++) {
                sb.append(IotDBVisitor.asString(gen.generateConstantForIotDBDataType(columns.get(c).getType())))
                        .append(", ");
            }
            sb.delete(sb.length() - 2, sb.length())
                    .append(")").append(",");
        }
        IotDBErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.substring(0, sb.length() - 1), errors, false);
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
