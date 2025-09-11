package com.fuzzy.iotdb;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fuzzy.GlobalState;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.schema.*;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable.EncodingType;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable.TableInfoColumnName;
import com.fuzzy.iotdb.ast.IotDBConstant;
import com.fuzzy.iotdb.ast.IotDBOrderByTerm;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import com.fuzzy.iotdb.util.IotDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class IotDBSchema extends AbstractSchema<IotDBGlobalState, IotDBTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public IotDBSchema(List<IotDBTable> databaseTables) {
        super(databaseTables);
    }

    public enum SchemaInfoColumnName {
        Database,
        TTL,
        SchemaReplicationFactor,
        DataReplicationFactor,
        TimePartitionInterval
    }

    public static IotDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        // 查询表结构 -> show timeseries.**
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                Map<String, IotDBTable> tables = new HashMap<>();
                try (TSFuzzyStatement s = con.createStatement()) {
                    try (IotDBResultSet iotDBResultSet = (IotDBResultSet)
                            s.executeQuery(String.format("show timeseries %s.**", databaseName))) {
                        Map<String, Integer> columnOrdinalMap = iotDBResultSet.getColumnOrdinalMap();
                        List<String> columnTypes = iotDBResultSet.getColumnTypes();
                        while (iotDBResultSet.hasNext()) {
                            RowRecord rowRecord = iotDBResultSet.getCurrentValue();
                            String timeSeriesName = rowRecord.getFields().get(
                                    columnOrdinalMap.get(TableInfoColumnName.Timeseries.toString())).getStringValue();
                            // TODO Table信息需要加上其他字段，诸如Encoding, Compression
                            List<IotDBColumn> databaseColumns = parseRowRecordToColumns(rowRecord, columnTypes,
                                    columnOrdinalMap);
                            String tableName = getTableNameFromTimeSeriesName(timeSeriesName);
                            IotDBTable t;
                            if (tables.containsKey(tableName)) {
                                IotDBTable tempTable = tables.get(tableName);
                                databaseColumns.addAll(tempTable.getColumns());
                                t = new IotDBTable(tableName, databaseName, databaseColumns, null);
                            } else t = new IotDBTable(tableName, databaseName, databaseColumns, null);
                            for (IotDBColumn c : databaseColumns) c.setTable(t);
                            tables.put(tableName, t);
                        }
                    }
                }
                return new IotDBSchema(new ArrayList<>(tables.values()));
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<IotDBColumn> parseRowRecordToColumns(RowRecord rowRecord, List<String> columnTypes,
                                                             Map<String, Integer> columnOrdinalMap) throws SQLException {
        // columns: tags + time + field
        List<IotDBColumn> columns = new ArrayList<>();

        String tags = rowRecord.getFields().get(
                columnOrdinalMap.get(TableInfoColumnName.Tags.toString())).getStringValue();
        if (!tags.equalsIgnoreCase(IotDBValueStateConstant.NULL.getValue())) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                HashMap<String, String> tagMap = objectMapper.readValue(tags, HashMap.class);
                tagMap.keySet().forEach(tagKey -> {
                    // TODO 暂时先不将tags放置column中
//                    columns.add(new IotDBColumn(tagKey, true, IotDBDataType.TEXT));
                });
            } catch (IOException e) {
                log.error("IotDB解析Tags异常, tags:{} e:", tags, e);
                throw new SQLException(e.getMessage());
            }
        }

//        IotDBColumn timeColumn = new IotDBColumn(IotDBValueStateConstant.TIMESTAMP.getValue(), false,
//                IotDBDataType.INT64);
//        columns.add(timeColumn);

        String timeSeriesName = rowRecord.getFields().get(
                columnOrdinalMap.get(TableInfoColumnName.Timeseries.toString())).getStringValue();
        int index = timeSeriesName.lastIndexOf(".");
        assert index != -1;
        IotDBColumn fieldColumn = new IotDBColumn(getColumnNameFromTimeSeriesName(timeSeriesName), false,
                IotDBDataType.getDataTypeByName(rowRecord.getFields().get(columnOrdinalMap.get(
                        TableInfoColumnName.DataType.toString())).getStringValue()));
        columns.add(fieldColumn);
        return columns;
    }

    private static String getTableNameFromTimeSeriesName(String timeSeriesName) {
        int columnIndex = timeSeriesName.lastIndexOf(".");
        int tableIndex = timeSeriesName.substring(0, columnIndex).lastIndexOf(".");
        return timeSeriesName.substring(tableIndex + 1, columnIndex);
    }

    private static String getColumnNameFromTimeSeriesName(String timeSeriesName) {
        int index = timeSeriesName.lastIndexOf(".");
        return timeSeriesName.substring(index + 1);
    }

    public static class IotDBTables extends AbstractTables<IotDBTable, IotDBColumn> {

        private List<String> orderByColumn;
        private IotDBTable iotDBTable;

        public IotDBTables(List<IotDBTable> tables) {
            super(tables);
            this.iotDBTable = Randomly.fromList(tables);
        }

        private String getSelectElement() {
            orderByColumn = new ArrayList<>();
            orderByColumn.add(IotDBValueStateConstant.TIME_FIELD.getValue());

            StringBuilder sb = new StringBuilder();
            iotDBTable.getColumns().forEach(column -> {
                sb.append(column.getName())
                        .append(" AS ")
                        .append(column.getName())
                        .append(", ");
            });
            orderByColumn.add(IotDBValueStateConstant.TIME_FIELD.getValue());
            return sb.substring(0, sb.length() - 2).replace(".", "_");
        }

        @Override
        public String tableNamesAsString() {
            return iotDBTable.getDatabaseName() + "." + iotDBTable.getName();
        }

        public IotDBRowValue getRandomRowValue(GlobalState globalState) throws SQLException {
            SQLConnection con = (SQLConnection) globalState.getConnection();
            long rowCount = iotDBTable.getNrRows((IotDBGlobalState) globalState);
            // IotDB c.getTable().getName()即是某段时序, 挑选出的数据可包括固定的tags, 随机的Field和Time
            String randomRow = String.format("SELECT %s FROM %s ORDER BY %s %s LIMIT 1 OFFSET %d", getSelectElement(),
                    tableNamesAsString(), Randomly.fromList(orderByColumn),
                    IotDBOrderByTerm.IotDBOrder.getRandomOrder().toString(),
                    globalState.getRandomly().getLong(0, rowCount));
            Map<IotDBColumn, IotDBConstant> values = new HashMap<>();
            try (IotDBStatement s = (IotDBStatement) con.createStatement()) {
                IotDBResultSet randomRowValues = (IotDBResultSet) s.executeQuery(randomRow);
                if (!randomRowValues.hasNext()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }

                // 时间戳字段设置表信息(get(0)) -> 该数据库下任意表均有该字段
                Timestamp timestamp = randomRowValues.getTimestamp();
                IotDBColumn timeColumn = new IotDBColumn(IotDBValueStateConstant.TIME_FIELD.getValue(), false,
                        IotDBDataType.INT64);
                timeColumn.setTable(iotDBTable);
                values.put(timeColumn, IotDBConstant.createInt64Constant(timestamp.getTime()));
                for (int i = 0; i < iotDBTable.getColumns().size(); i++) {
                    IotDBColumn column = iotDBTable.getColumns().get(i);
                    // 时间戳单独考虑(不同table均具备timeColumn列, 实际上一条select查询，其结果time列是共用的)
//                    if (column.getName().equalsIgnoreCase(IotDBValueStateConstant.TIMESTAMP.getValue()))
//                        continue;
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getName());
                    IotDBConstant constant;
                    if (randomRowValues.getString(columnIndex).equalsIgnoreCase(IotDBValueStateConstant.NULL.getValue())) {
                        constant = IotDBConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT32:
                                value = randomRowValues.getInt(columnIndex);
                                constant = IotDBConstant.createInt32Constant((int) value);
                                break;
                            case INT64:
                                value = randomRowValues.getLong(columnIndex);
                                constant = IotDBConstant.createInt64Constant((long) value);
                                break;
                            case TEXT:
                                value = randomRowValues.getString(columnIndex);
                                constant = IotDBConstant.createStringConstant((String) value);
                                break;
                            case DOUBLE:
                                value = randomRowValues.getDouble(columnIndex);
                                constant = IotDBConstant.createDoubleConstant((double) value);
                                break;
                            default:
                                throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.hasNext();
                return new IotDBRowValue(this, values);
            }
        }

    }

    public IotDBTables getRandomTableNonEmptyTables() {
        return new IotDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public IotDBTables getOneRandomTableNonEmptyTables() {
        return new IotDBTables(Randomly.nonEmptySubset(getDatabaseTables(), 1));
    }

    public static class IotDBTable extends AbstractRelationalTable<IotDBColumn, IotDBIndex, IotDBGlobalState> {

        public IotDBTable(String name, String databaseName, List<IotDBColumn> columns, List<IotDBIndex> indexes) {
            super(name, databaseName, columns, indexes, false);
        }

        @Override
        public String getSelectCountTableName() {
            return String.format("%s.%s", databaseName, getName());
        }

        @Override
        public int getSelectCountIndex() {
            return 0;
        }

        public enum TableInfoColumnName {
            Timeseries,
            Alias,
            Database,
            DataType,
            Encoding,
            Compression,
            Tags,
            Attributes,
            Deadband,
            DeadbandParameters,
            ViewType
        }

        public enum EncodingType {
            PLAIN,
            TS_2DIFF,
            RLE,
            GORILLA,
            DICTIONARY,
            ZIGZAG,
            CHIMP,
            SPRINTZ,
            RLBE
        }

        public enum CompressionType {
            UNCOMPRESSED,
            SNAPPY,
            LZ4,
            GZIP,
            ZSTD,
            LZMA2
        }
    }

    public static class IotDBColumn extends AbstractTableColumn<IotDBTable, IotDBDataType> {

        public IotDBColumn(String name, boolean isTag, IotDBDataType type) {
            super(name, null, type, isTag);
        }

    }

    public static final class IotDBIndex extends TableIndex {

        protected IotDBIndex(String indexName) {
            super(indexName);
        }

    }

    public static class IotDBRowValue extends AbstractRowValue<IotDBTables, IotDBColumn, IotDBConstant> {

        IotDBRowValue(IotDBTables tables, Map<IotDBColumn, IotDBConstant> values) {
            super(tables, values);
        }

    }

    public enum IotDBDataType {
        BOOLEAN("BOOLEAN"), INT32("INT32"), INT64("INT64"),
        FLOAT("FLOAT"), DOUBLE("DOUBLE"), TEXT("TEXT"), NULL("NULL"),
        BIGDECIMAL("BIGDECIMAL");

        private String typeName;
        private List<EncodingType> supportedEncoding = new ArrayList<>();

        IotDBDataType(String typeName) {
            this.typeName = typeName;
            switch (typeName) {
                case "BOOLEAN":
                    supportedEncoding.add(EncodingType.PLAIN);
                    supportedEncoding.add(EncodingType.RLE);
                    break;
                case "INT32":
                case "INT64":
                    supportedEncoding.add(EncodingType.PLAIN);
                    supportedEncoding.add(EncodingType.RLE);
                    supportedEncoding.add(EncodingType.TS_2DIFF);
                    supportedEncoding.add(EncodingType.GORILLA);
                    supportedEncoding.add(EncodingType.ZIGZAG);
                    supportedEncoding.add(EncodingType.CHIMP);
                    supportedEncoding.add(EncodingType.SPRINTZ);
                    supportedEncoding.add(EncodingType.RLBE);
                    break;
                case "FLOAT":
                case "DOUBLE":
                    supportedEncoding.add(EncodingType.PLAIN);
                    supportedEncoding.add(EncodingType.RLE);
                    supportedEncoding.add(EncodingType.TS_2DIFF);
                    supportedEncoding.add(EncodingType.GORILLA);
                    supportedEncoding.add(EncodingType.CHIMP);
                    supportedEncoding.add(EncodingType.SPRINTZ);
                    supportedEncoding.add(EncodingType.RLBE);
                    break;
                case "TEXT":
                    supportedEncoding.add(EncodingType.PLAIN);
                    supportedEncoding.add(EncodingType.DICTIONARY);
                    break;
                case "NULL":
                case "BIGDECIMAL":
                    break;
                default:
                    throw new IllegalArgumentException(String.format("IotDBDataType该类型不存在, type:%s", typeName));
            }
        }

        public static IotDBDataType getDataTypeByName(String typeName) {
            for (IotDBDataType value : IotDBDataType.values()) {
                if (value.getTypeName().equalsIgnoreCase(typeName)) return value;
            }
            throw new IllegalArgumentException(String.format("IotDBDataType该类型不存在:%s", typeName));
        }

        public String getTypeName() {
            return typeName;
        }

        public List<EncodingType> getSupportedEncoding() {
            return supportedEncoding;
        }

        public static IotDBSchema.IotDBDataType[] valuesPQS() {
            return new IotDBSchema.IotDBDataType[]{INT32, INT64, DOUBLE, TEXT};
        }

        public static IotDBSchema.IotDBDataType[] valuesTSAF() {
            return new IotDBSchema.IotDBDataType[]{INT32, INT64, DOUBLE};
        }

        public static IotDBDataType getRandom(IotDBGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(valuesPQS());
            } else if (globalState.usesTSAF() || globalState.usesHINT()) {
                return Randomly.fromOptions(valuesTSAF());
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public TSAFDataType toTSAFDataType() {
            switch (this) {
                case INT32:
                    return TSAFDataType.INT;
                case INT64:
                    return TSAFDataType.BIGINT;
                case DOUBLE:
                    return TSAFDataType.DOUBLE;
                default:
                    throw new AssertionError();
            }
        }

        public static IotDBSchema.IotDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public boolean isNumeric() {
            switch (this) {
                case INT32:
                case INT64:
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return true;
                case TEXT:
                case BOOLEAN:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isInt() {
            switch (this) {
                case INT32:
                case INT64:
                    return true;
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                case TEXT:
                case BOOLEAN:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isEquals(IotDBDataType rightDataType) {
            if (this.typeName.equals(rightDataType.getTypeName())) return true;
            else if (this.isNumeric() && rightDataType.isNumeric()) return true;
            else return false;
        }
    }

}
