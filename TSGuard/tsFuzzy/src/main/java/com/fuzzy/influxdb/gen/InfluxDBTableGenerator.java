package com.fuzzy.influxdb.gen;

import com.fuzzy.Randomly;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InfluxDBTableGenerator {
    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private final Randomly r;
    private final List<String> columns = new ArrayList<>();
    private final InfluxDBSchema schema;
    private final InfluxDBGlobalState globalState;

    public InfluxDBTableGenerator(InfluxDBGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.r = globalState.getRandomly();
        this.schema = globalState.getSchema();
        this.globalState = globalState;
        InfluxDBInsertGenerator.addLastTimestamp(globalState.getDatabaseName(), tableName,
                globalState.getOptions().getStartTimestampOfTSData());
        SamplingFrequencyManager.getInstance().addSamplingFrequency(globalState.getDatabaseName(), tableName,
                globalState.getOptions().getStartTimestampOfTSData(),
                InfluxDBInsertGenerator.SAMPLING_NUMBER * globalState.getOptions().getSamplingFrequency(),
                (long) InfluxDBInsertGenerator.SAMPLING_NUMBER);
    }

    public static SQLQueryAdapter generate(InfluxDBGlobalState globalState, String tableName) {
        return new InfluxDBTableGenerator(globalState, tableName).create();
    }

    private SQLQueryAdapter create() {
        // 插入一条初始时序数值 -> 创建时间序列数据, 创建table
        ExpectedErrors errors = new ExpectedErrors();
        sb.append(tableName);
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            sb.append(",");
            appendColumn(i, true);
        }

        sb.append(" ");
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            appendColumn(i, false);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        sb.append(" ");
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection(globalState.getDatabaseName(), tableName);
        long startTimestamp = globalState.getOptions().getStartTimestampOfTSData();
        long endTimestamp = startTimestamp + InfluxDBInsertGenerator.SAMPLING_NUMBER *
                globalState.getOptions().getSamplingFrequency();
        List<Long> timestamps = samplingFrequency.apply(startTimestamp, endTimestamp);
        long initTimestamp = timestamps.get(0);
        sb.append(initTimestamp);
        appendTableOptions();
        appendPartitionOptions();
        addCommonErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addCommonErrors(ExpectedErrors list) {

    }

    private enum PartitionOptions {
        HASH, KEY
    }

    private void appendPartitionOptions() {

    }

    private enum TableOptions {
        AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, COMPRESSION, DELAY_KEY_WRITE, /* ENCRYPTION, */ ENGINE, INSERT_METHOD,
        KEY_BLOCK_SIZE, MAX_ROWS, MIN_ROWS, PACK_KEYS, STATS_AUTO_RECALC, STATS_PERSISTENT, STATS_SAMPLE_PAGES;

        public static List<TableOptions> getRandomTableOptions() {
            List<TableOptions> options;
            // try to ensure that usually, only a few of these options are generated
            if (Randomly.getBooleanWithSmallProbability()) {
                options = Randomly.subset(TableOptions.values());
            } else {
                if (Randomly.getBoolean()) {
                    options = Collections.emptyList();
                } else {
                    options = Randomly.nonEmptySubset(Arrays.asList(TableOptions.values()), Randomly.smallNumber());
                }
            }
            return options;
        }
    }

    private void appendTableOptions() {

    }

    private void appendColumn(int columnId, boolean isTag) {
        String columnName = DBMSCommon.createColumnName(columnId, isTag, tableName);
        columns.add(columnName);
        sb.append(columnName);
        if (isTag) sb.append("=initTag");
        else {
            InfluxDBDataType randomType = InfluxDBDataType.getRandom(globalState);
            sb.append("=");
            sb.append(randomType.getInitValueByType(globalState));
        }
        appendColumnDefinition();
    }

    private enum ColumnOptions {
        NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
    }

    private void appendColumnDefinition() {
    }

    private void appendType(InfluxDBDataType randomType) {
        // TODO
        switch (randomType) {
            case BOOLEAN:
                sb.append("DECIMAL");
                optionallyAddPrecisionAndScale(sb);
                break;
            case INT:
                sb.append(Randomly.fromOptions("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"));
                if (Randomly.getBoolean()) {
                    sb.append("(");
                    sb.append(Randomly.getNotCachedInteger(0, 255)); // Display width out of range for column 'c0' (max =
                    // 255)
                    sb.append(")");
                }
                break;
            case STRING:
                sb.append(Randomly.fromOptions("VARCHAR(500)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"));
                break;
            case FLOAT:
                sb.append("FLOAT");
                optionallyAddPrecisionAndScale(sb);
                break;
            case UINT:
                sb.append(Randomly.fromOptions("DOUBLE", "FLOAT"));
                optionallyAddPrecisionAndScale(sb);
                break;
            default:
                throw new AssertionError();
        }
        if (randomType.isNumeric()) {
            if (Randomly.getBoolean() && randomType != InfluxDBDataType.INT) {
                sb.append(" UNSIGNED");
            }
            if (!globalState.usesPQS() && Randomly.getBoolean()) {
                sb.append(" ZEROFILL");
            }
        }
    }

    public static void optionallyAddPrecisionAndScale(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            sb.append("(");
            // The maximum number of digits (M) for DECIMAL is 65
            long m = Randomly.getNotCachedInteger(1, 65);
            sb.append(m);
            sb.append(", ");
            // The maximum number of supported decimals (D) is 30
            long nCandidate = Randomly.getNotCachedInteger(1, 30);
            // For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'c0').
            long n = Math.min(nCandidate, m);
            sb.append(n);
            sb.append(")");
        }
    }

}
