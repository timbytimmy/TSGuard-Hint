package com.fuzzy.griddb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.griddb.GridDBGlobalState;
import com.fuzzy.griddb.GridDBSchema.GridDBDataType;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GridDBTableGenerator {
    private final StringBuilder sb = new StringBuilder();
    private final boolean allowPrimaryKey;
    private boolean setPrimaryKey;
    private final String tableName;
    private final List<String> columns = new ArrayList<>();
    private final GridDBGlobalState globalState;

    public GridDBTableGenerator(GridDBGlobalState globalState, String tableName) {
        this.tableName = tableName;
        allowPrimaryKey = Randomly.getBoolean();
        this.globalState = globalState;
        String databaseName = globalState.getDatabaseName();
        SamplingFrequencyManager.getInstance().addSamplingFrequency(databaseName,
                String.format("%s_%s", databaseName, tableName), globalState.getOptions().getStartTimestampOfTSData(),
                30 * globalState.getOptions().getSamplingFrequency(), 30L);
    }

    public static SQLQueryAdapter generate(GridDBGlobalState globalState, String tableName) {
        return new GridDBTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        createOrdinaryTable();
        return new SQLQueryAdapter(sb.toString(), new ExpectedErrors(), true);
    }

    public void createOrdinaryTable() {
        String name = globalState.getDatabaseName() + "_" + tableName;
        sb.append("CREATE");
        sb.append(" TABLE");
        if (Randomly.getBoolean()) sb.append(" IF NOT EXISTS");
        sb.append(" ");
        sb.append(name);
        sb.append(String.format("( %s %s PRIMARY KEY, ", GridDBConstantString.TIME_FIELD_NAME.getName(),
                GridDBDataType.TIMESTAMP.getTypeName()));
        if (globalState.usesTSAF())
            sb.append(String.format("%s INT, ", GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName()));
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) sb.append(", ");
            appendColumn(i);
        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            sb.append(" USING TIMESERIES ");
//            if (Randomly.getBoolean()) appendTableOptions();
        }
    }

    private void appendColumn(int columnId) {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinition();
    }

    private void appendColumnDefinition() {
        sb.append(" ");
        GridDBDataType randomType = GridDBDataType.getRandom(globalState);
        sb.append(randomType.getTypeName());
        sb.append(" ");
        // TODO
//        appendColumnOption(randomType);
    }

    private enum ColumnOptions {
        NULL, NOT_NULL, PRIMARY_KEY, // TODO ENCODE, COMPRESS, LEVEL
    }

    private void appendColumnOption(GridDBDataType type) {
        Randomly randomly = globalState.getRandomly();
        boolean isNull = false;
        List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
        for (ColumnOptions o : columnOptions) {
            sb.append(" ");
            switch (o) {
                case NULL:
                    sb.append("NULL");
                    break;
                case PRIMARY_KEY:
                    // PRIMARY KEYs cannot be NULL
                    if (allowPrimaryKey && !setPrimaryKey && !isNull) {
                        sb.append("PRIMARY KEY");
                        setPrimaryKey = true;
                    }
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    // USING TIMESERIES
    private enum TableOptions {
        expiration_time_unit, expiration_division_count;
    }

    private enum ExpirationTimeUnit {
        DAY, HOUR, MINUTE
    }

    private void appendTableOptions() {
        sb.append("WITH (expiration_type=ROW, expiration_time=")
                .append(globalState.getRandomly().getInteger(1, 100)).append(", ");
        List<TableOptions> tableOptions = Randomly.subset(TableOptions.values());
        for (TableOptions o : tableOptions) {
            switch (o) {
                case expiration_time_unit:
                    sb.append(String.format("expiration_time_unit=%s, ",
                            Randomly.fromOptions(ExpirationTimeUnit.values()).name()));
                    break;
                case expiration_division_count:
                    sb.append(String.format("expiration_division_count=%d, ",
                            globalState.getRandomly().getInteger(1, 16)));
                    break;
                default:
                    throw new AssertionError(o);
            }
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(")");
    }
}
