package com.fuzzy.TDengine;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.GlobalState;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.TDengine.TDengineSchema.TDengineTable;
import com.fuzzy.TDengine.ast.TDengineConstant;
import com.fuzzy.TDengine.resultSet.TDengineResultSet;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.schema.*;
import com.fuzzy.common.tsaf.TSAFDataType;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TDengineSchema extends AbstractSchema<TDengineGlobalState, TDengineTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public TDengineSchema(List<TDengineTable> databaseTables) {
        super(databaseTables);
    }

    public static TDengineSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        // select * from INFORMATION_SCHEMA.INS_TAGS;
        // select * from INFORMATION_SCHEMA.INS_COLUMNS;
        // select * from INFORMATION_SCHEMA.INS_TABLES;
        // select * from INFORMATION_SCHEMA.INS_DATABASES;
        Exception ex = null;

        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                // databaseName = "bucket";
                List<TDengineTable> databaseTables = new ArrayList<>();
                try (TSFuzzyStatement s = con.createStatement()) {
                    try (TDengineResultSet rs = (TDengineResultSet) s.executeQuery(String.format(
                            "select * from INFORMATION_SCHEMA.INS_TABLES where db_name = '%s'", databaseName))) {
                        while (rs.hasNext()) {
                            // 多张表
                            String tableName = rs.getString(TDengineFieldName.TABLE_NAME.getFieldName());
//                            int columnCount = rs.getInt(TDengineFieldName.COLUMN_COUNT.getFieldName());
                            TDengineResultSet columnsResult = (TDengineResultSet) s.executeQuery(String.format(
                                    "select * from INFORMATION_SCHEMA.INS_COLUMNS where db_name = '%s'" +
                                            " and table_name = '%s'", databaseName, tableName));

                            // Columns
                            List<TDengineColumn> columns = new ArrayList<>();
                            while (columnsResult.hasNext()) {
                                String columnName = columnsResult.getString(TDengineFieldName.COL_NAME.getFieldName());
                                String columnType = columnsResult.getString(TDengineFieldName.COL_TYPE.getFieldName());
//                                int columnLen = columnsResult.getInt(TDengineFieldName.COL_LENGTH.getFieldName());
                                columns.add(new TDengineColumn(columnName, false,
                                        TDengineDataType.getInstanceFromValue(columnType)));
                            }
                            // tag?

                            // table
                            TDengineTable t = new TDengineTable(tableName, databaseName, columns, null);
                            for (TDengineColumn c : columns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new TDengineSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    public enum TDengineFieldName {
        TABLE_NAME("table_name"), DB_NAME("db_name"), COLUMN_COUNT("columns"),
        COL_NAME("col_name"), COL_TYPE("col_type"), COL_NULLABLE("col_nullable"), COL_LENGTH("col_length");

        private String fieldName;

        private TDengineFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    public enum TDengineDataType {
        INT("INT"), UINT("INT UNSIGNED"), TIMESTAMP("TIMESTAMP"),
        BIGINT("BIGINT"), UBIGINT("BIGINT UNSIGNED"), FLOAT("FLOAT"),
        DOUBLE("DOUBLE"), BINARY("BINARY"), SMALLINT("SMALLINT"),
        USMALLINT("SMALLINT UNSIGNED"), TINYINT("TINYINT"), UTINYINT("TINYINT UNSIGNED"),
        BOOL("BOOL"), NCHAR("NCHAR"), JSON("JSON"),
        VARCHAR("VARCHAR"), GEOMETRY("GEOMETRY"), VARBINARY("VARBINARY"),
        NULL("NULL"), BIGDECIMAL("BIGDECIMAL");

        private String typeName;

        TDengineDataType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static TDengineDataType[] valuesPQS() {
//            return new TDengineDataType[]{INT, UINT, BIGINT, UBIGINT, BINARY, VARCHAR, DOUBLE};
            return new TDengineDataType[]{INT, UINT, BIGINT, UBIGINT, BINARY, VARCHAR};
        }

        public static TDengineDataType[] valuesTSAF() {
            return new TDengineDataType[]{INT};
        }

        public static TDengineDataType getRandom(TDengineGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(valuesPQS());
            } else if (globalState.usesTSAF()) {
                return Randomly.fromOptions(valuesTSAF());
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public static TDengineDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public TSAFDataType toTSAFDataType() {
            switch (this) {
                case INT:
                    return TSAFDataType.INT;
                case UINT:
                    return TSAFDataType.UINT;
                case BIGINT:
                    return TSAFDataType.BIGINT;
                case UBIGINT:
                    return TSAFDataType.UBIGINT;
                case DOUBLE:
                    return TSAFDataType.DOUBLE;
                default:
                    throw new AssertionError();
            }
        }

        public Object getInitValueByType(GlobalState globalState) {
            Randomly randomly = globalState.getRandomly();
            switch (this) {
                case INT:
                    return randomly.getInteger();
                case UINT:
                    return randomly.getInteger();
                case BIGINT:
                case UBIGINT:
                    return randomly.getLong();
                case BINARY:
                case VARCHAR:
                    return randomly.getString().replace("\\", "").replace("\n", "");
                case BOOL:
                    return Randomly.getBoolean();
                case DOUBLE:
                    return randomly.getInfiniteDouble();
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case INT:
                case UINT:
                case BIGINT:
                case UBIGINT:
                case TINYINT:
                case UTINYINT:
                case SMALLINT:
                case USMALLINT:
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return true;
                case BINARY:
                case VARCHAR:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isInt() {
            switch (this) {
                case INT:
                case UINT:
                case BIGINT:
                case UBIGINT:
                case TINYINT:
                case UTINYINT:
                case SMALLINT:
                case USMALLINT:
                    return true;
                default:
                    return false;
            }
        }

        public boolean isIntOrString() {
            switch (this) {
                case INT:
                case UINT:
                case BIGINT:
                case UBIGINT:
                case TINYINT:
                case UTINYINT:
                case SMALLINT:
                case USMALLINT:
                case BINARY:
                case VARCHAR:
                    return true;
                default:
                    return false;
            }
        }

        public static TDengineDataType getInstanceFromValue(String typeName) {
            String splitTypeName = typeName.split("\\(")[0];
            for (TDengineDataType value : TDengineDataType.values()) {
                if (value.getTypeName().equals(splitTypeName)) return value;
            }
            throw new IllegalArgumentException("该类型不存在:" + typeName);
        }

        public static TDengineDataType getDataTypeByName(String typeName) {
            for (TDengineDataType value : TDengineDataType.values()) {
                if (value.getTypeName().equalsIgnoreCase(typeName)) return value;
            }
            throw new IllegalArgumentException("该类型不存在:" + typeName);
        }
    }

    public static class TDengineColumn extends AbstractTableColumn<TDengineTable, TDengineDataType> {
        public TDengineColumn(String name, boolean isTag, TDengineDataType type) {
            super(name, null, type, isTag);
        }
    }

    public static class TDengineTable extends AbstractRelationalTable<TDengineColumn, TDengineIndex, TDengineGlobalState> {
        public TDengineTable(String name, String databaseName, List<TDengineColumn> columns, List<TDengineIndex> indexes) {
            super(name, databaseName, columns, indexes, false);
        }

        public List<TDengineColumn> getRandomNonEmptyColumnSubsetContainsPrimaryKey() {
            List<TDengineColumn> requiredColumns = getColumns().stream().filter(column -> {
                return column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        || column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());

            List<TDengineColumn> randomColumns = getColumns().stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());
            requiredColumns.addAll(Randomly.nonEmptySubset(randomColumns));
            return requiredColumns;
        }

        public List<TDengineColumn> getAllColumnSubsetContainsPrimaryKey() {
            List<TDengineColumn> primaryColumns = getColumns().stream().filter(column -> {
                return column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        || column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());

            List<TDengineColumn> columns = getColumns().stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());
            primaryColumns.addAll(columns);
            return primaryColumns;
        }
    }

    public static final class TDengineIndex extends TableIndex {

        protected TDengineIndex(String indexName) {
            super(indexName);
        }

    }

    public static class TDengineRowValue extends AbstractRowValue<TDengineTables, TDengineColumn, TDengineConstant> {
        TDengineRowValue(TDengineTables tables, Map<TDengineColumn, TDengineConstant> values) {
            super(tables, values);
        }
    }

    public static class TDengineTables extends AbstractTables<TDengineTable, TDengineColumn> {

        public TDengineTables(List<TDengineTable> tables) {
            super(tables);
        }

        public TDengineRowValue getRandomRowValue(GlobalState globalState) throws SQLException {
            SQLConnection con = (SQLConnection) globalState.getConnection();
            // TDengine 不支持 cross join, 故 其查询table一次仅允许一张表
            assert getTables().size() == 1;
            long rowCount = this.getTables().get(0).getNrRows((TDengineGlobalState) globalState);
            TDengineColumn orderByColumn = Randomly.fromList(getColumns());
            // 以OFFSET模拟随机字段
            String randomRow = String.format("SELECT %s FROM %s ORDER BY %s LIMIT 1 OFFSET %d", columnNamesAsString(
                            AbstractTableColumn::getName), tableNamesAsString(), orderByColumn.getName(),
                    globalState.getRandomly().getLong(0, rowCount));
            Map<TDengineColumn, TDengineConstant> values = new HashMap<>();
            try (TSFuzzyStatement s = con.createStatement()) {
                TDengineResultSet randomRowValues = (TDengineResultSet) s.executeQuery(randomRow);
                if (!randomRowValues.hasNext()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    TDengineColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getName());
                    assert columnIndex == i + 1;
                    TDengineConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = TDengineConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = TDengineConstant.createInt32Constant((long) value);
                                break;
                            case UINT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = TDengineConstant.createUInt32Constant((long) value);
                                break;
                            case TIMESTAMP:
                                // TODO
                            case UBIGINT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = TDengineConstant.createUInt64Constant((long) value);
                                break;
                            case BIGINT:
                                value = randomRowValues.getLong(columnIndex);
                                constant = TDengineConstant.createInt64Constant((long) value);
                                break;
                            case BINARY:
                            case VARCHAR:
                                value = randomRowValues.getString(columnIndex);
                                constant = TDengineConstant.createStringConstant((String) value);
                                break;
                            case DOUBLE:
                                value = randomRowValues.getDouble(columnIndex);
                                constant = TDengineConstant.createDoubleConstant((double) value);
                                break;
                            default:
                                throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new TDengineRowValue(this, values);
            }
        }

    }

    public TDengineTables getRandomTableNonEmptyTables() {
        // TDengine 不支持 cross join, 故 其查询table一次仅允许一张表
        return new TDengineTables(Randomly.nonEmptySubset(getDatabaseTables(), 1));
    }
}
