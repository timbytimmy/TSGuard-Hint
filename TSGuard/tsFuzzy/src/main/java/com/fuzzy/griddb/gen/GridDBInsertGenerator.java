package com.fuzzy.griddb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.common.tsaf.EquationsManager;
import com.fuzzy.common.tsaf.TableToNullValuesManager;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.common.util.TimeUtil;
import com.fuzzy.griddb.GridDBErrors;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema;
import com.fuzzy.griddb.GridDBSchema.GridDBColumn;
import com.fuzzy.griddb.GridDBSchema.GridDBTable;
import com.fuzzy.griddb.GridDBVisitor;
import com.fuzzy.griddb.ast.GridDBConstant;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class GridDBInsertGenerator {

    private final GridDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final GridDBGlobalState globalState;
    // database_table -> randomGenTime
    private static final Map<String, Boolean> isRandomlyGenerateTimestamp = new HashMap<>();
    // database_table -> lastTimestamp
    private static final Map<String, Long> lastTimestamp = new HashMap<>();

    public GridDBInsertGenerator(GridDBGlobalState globalState, GridDBTable table) {
        this.globalState = globalState;
        this.table = table;
        String hashKey = generateHashKey(globalState.getDatabaseName(), table.getName());
        if (!isRandomlyGenerateTimestamp.containsKey(hashKey)) {
            if (globalState.usesTSAF()) isRandomlyGenerateTimestamp.put(hashKey, false);
            else isRandomlyGenerateTimestamp.put(hashKey, Randomly.getBoolean());
            lastTimestamp.put(hashKey, globalState.getOptions().getStartTimestampOfTSData());
        }
    }

    public static SQLQueryAdapter insertRow(GridDBGlobalState globalState) throws SQLException {
        GridDBTable table = globalState.getSchema().getRandomTable();
        return insertRow(globalState, table);
    }

    public static SQLQueryAdapter insertRow(GridDBGlobalState globalState, GridDBTable table) throws SQLException {
        return new GridDBInsertGenerator(globalState, table).generateInsert();
    }

    private SQLQueryAdapter generateInsert() {
        if (Randomly.getBoolean()) sb.append("INSERT OR REPLACE");
        else sb.append("REPLACE");
        sb.append(" INTO ");
        sb.append(table.getName());

        if (globalState.usesTSAF()) generateIntoForTSAF();
        else generateInto();

        GridDBErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private void generateInto() {
        List<GridDBColumn> columns = table.getAllColumnSubsetContainsPrimaryKey();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");

        int nrRows;
        if (Randomly.getBoolean()) nrRows = 1;
        else nrRows = 1 + Randomly.smallNumber();
        GridDBExpressionGenerator gen = new GridDBExpressionGenerator(globalState);
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) sb.append(", ");
            sb.append("(");
            // timestamp -> 按照采样间隔顺序插入 or 随机生成(起始时间戳 -> 至今)
            String databaseAndTableName = generateHashKey(globalState.getDatabaseName(), table.getName());
            long nextTimestamp;
            if (isRandomlyGenerateTimestamp.get(databaseAndTableName)) {
                nextTimestamp = globalState.getRandomTimestamp();
            } else {
                nextTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
                lastTimestamp.put(databaseAndTableName, nextTimestamp);
            }
            sb.append("TIMESTAMP('").append(TimeUtil.timestampToISO8601(nextTimestamp)).append("')");
            for (int c = 1; c < columns.size(); c++) {
                sb.append(", ").append(
                        GridDBVisitor.asString(gen.generateConstantForGridDBDataType(columns.get(c).getType())));
            }
            sb.append(")");
        }
    }

    private void generateIntoForTSAF() {
        List<GridDBColumn> columns = table.getAllColumnSubsetContainsPrimaryKey();
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");

        String databaseName = globalState.getDatabaseName();
        String tableName = table.getName();
        int nrRows = 30;
        String databaseAndTableName = generateHashKey(databaseName, tableName);
        long startTimestamp = globalState.getNextSampleTimestamp(lastTimestamp.get(databaseAndTableName));
        long endTimestamp = startTimestamp + nrRows * globalState.getOptions().getSamplingFrequency();
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(databaseName, table.getName());
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        lastTimestamp.put(databaseAndTableName, endTimestamp);
        for (int row = 0; row < nrRows; row++) {
            long nextTimestamp = timestamps.get(row);
            // device id
            int deviceNumber = 1;
            while (deviceNumber > 0) {
                sb.append("(");
                // columns
                boolean isNull = false;
                // 空值概率存在
                if (Randomly.getBoolean(1, 50)) {
                    TableToNullValuesManager.addNullValueToTable(databaseName, tableName, nextTimestamp);
                    isNull = true;
                }
                for (int c = 0; c < columns.size(); c++) {
                    String columnName = columns.get(c).getName();
                    GridDBSchema.GridDBDataType type = columns.get(c).getType();
                    if (columnName.equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName())) {
                        // timestamp -> 按照采样间隔顺序插入(起始时间戳 -> 至今)
                        sb.append("TIMESTAMP('").append(TimeUtil.timestampToISO8601(nextTimestamp)).append("')");
                    } else if (columnName.equalsIgnoreCase(
                            GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName())) {
                        sb.append(", ").append(deviceNumber);
                    } else {
                        if (isNull) sb.append(", null");
                        else
                            sb.append(", ").append(GridDBConstant.createBigDecimalConstant(EquationsManager.getInstance()
                                    .initEquationsFromTimeSeries(databaseName, tableName, columnName, type.toTSAFDataType())
                                    .genValueByTimestamp(samplingFrequency, nextTimestamp)).castAs(columns.get(c).getType()));
                    }
                }
                sb.append("), ");
                deviceNumber--;
            }
        }
        sb.delete(sb.length() - 2, sb.length());
    }

    public static Long getLastTimestamp(String databaseName, String tableName) {
        return lastTimestamp.get(generateHashKey(databaseName, tableName));
    }

    public static String generateHashKey(String databaseName, String tableName) {
        return String.format("%s_%s", databaseName, tableName);
    }
}
