package com.fuzzy.griddb;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.GlobalState;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.schema.*;
import com.fuzzy.common.tsaf.TSAFDataType;
import com.fuzzy.griddb.GridDBSchema.GridDBTable;
import com.fuzzy.griddb.ast.GridDBConstant;
import com.fuzzy.griddb.resultset.GridDBResultSet;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class GridDBSchema extends AbstractSchema<GridDBGlobalState, GridDBTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public GridDBSchema(List<GridDBTable> databaseTables) {
        super(databaseTables);
    }

    public static GridDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;

        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<GridDBTable> databaseTables = new ArrayList<>();
                DatabaseMetaData metaData = con.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null,
                        String.format("%s_%%", databaseName), new String[]{"TABLE"})) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        ResultSet columnResult = metaData.getColumns(null, null,
                                tableName, "%");

                        // Columns
                        List<GridDBColumn> columns = new ArrayList<>();
                        while (columnResult.next()) {
                            String columnName = columnResult.getString(GridDBFieldName.COL_NAME.getFieldName());
                            String columnType = columnResult.getString(GridDBFieldName.COL_TYPE.getFieldName());
                            columns.add(new GridDBColumn(columnName, GridDBDataType.getInstanceFromValue(columnType)));
                        }

                        // table
                        GridDBTable t = new GridDBTable(tableName, databaseName, columns, null);
                        columns.forEach(c -> c.setTable(t));
                        databaseTables.add(t);
                    }
                }
                return new GridDBSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    public enum GridDBFieldName {
        TABLE_NAME("TABLE_NAME"), COL_NAME("COLUMN_NAME"), COL_TYPE("TYPE_NAME");

        private String fieldName;

        private GridDBFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    public enum GridDBDataType {
        BOOL("BOOL"), STRING("STRING"), BYTE("BYTE"), SHORT("SHORT"),
        INTEGER("INTEGER"), LONG("LONG"), FLOAT("FLOAT"), DOUBLE("DOUBLE"),
        TIMESTAMP("TIMESTAMP"), GEOMETRY("GEOMETRY"), BLOB("BLOB"),
        BIGDECIMAL("BIGDECIMAL"), NULL("NULL");

        private String typeName;

        GridDBDataType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static GridDBDataType[] valuesPQS() {
            return new GridDBDataType[]{DOUBLE};
        }

        public static GridDBDataType[] valuesTSAF() {
            return new GridDBDataType[]{INTEGER, LONG, FLOAT, DOUBLE};
        }

        public static GridDBDataType getRandom(GridDBGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(valuesPQS());
            } else if (globalState.usesTSAF()) {
                return Randomly.fromOptions(valuesTSAF());
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public static GridDBDataType getRandom() {
            return Randomly.fromOptions(values());
        }

        public TSAFDataType toTSAFDataType() {
            switch (this) {
                case INTEGER:
                    return TSAFDataType.INT;
                case LONG:
                    return TSAFDataType.BIGINT;
                case FLOAT:
                case DOUBLE:
                    return TSAFDataType.DOUBLE;
                default:
                    throw new AssertionError();
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return true;
                case TIMESTAMP:
                case STRING:
                case BLOB:
                case BOOL:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public boolean isInt() {
            switch (this) {
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                    return true;
                default:
                    return false;
            }
        }

        public static GridDBDataType getInstanceFromValue(String typeName) {
            for (GridDBDataType value : GridDBDataType.values()) {
                if (value.getTypeName().equals(typeName)) return value;
            }
            throw new IllegalArgumentException("该类型不存在:" + typeName);
        }

        public static GridDBDataType getDataTypeByName(String typeName) {
            for (GridDBDataType value : GridDBDataType.values()) {
                if (value.getTypeName().equalsIgnoreCase(typeName)) return value;
            }
            throw new IllegalArgumentException("该类型不存在:" + typeName);
        }
    }

    public static class GridDBColumn extends AbstractTableColumn<GridDBTable, GridDBDataType> {
        public GridDBColumn(String name, GridDBDataType type) {
            super(name, null, type, false);
        }
    }

    public static class GridDBTable extends AbstractRelationalTable<GridDBColumn, GridDBIndex, GridDBGlobalState> {
        public GridDBTable(String name, String databaseName, List<GridDBColumn> columns, List<GridDBIndex> indexes) {
            super(name, databaseName, columns, indexes, false);
        }

        public List<GridDBColumn> getRandomNonEmptyColumnSubsetContainsPrimaryKey() {
            List<GridDBColumn> requiredColumns = getColumns().stream().filter(column -> {
                return column.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName())
                        || column.getName().equalsIgnoreCase(GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());

            List<GridDBColumn> randomColumns = getColumns().stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());
            requiredColumns.addAll(Randomly.nonEmptySubset(randomColumns));
            return requiredColumns;
        }

        public List<GridDBColumn> getAllColumnSubsetContainsPrimaryKey() {
            List<GridDBColumn> primaryColumns = getColumns().stream().filter(column -> {
                return column.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName())
                        || column.getName().equalsIgnoreCase(GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());

            List<GridDBColumn> columns = getColumns().stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(GridDBConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).collect(Collectors.toList());
            primaryColumns.addAll(columns);
            return primaryColumns;
        }
    }

    public static final class GridDBIndex extends TableIndex {

        protected GridDBIndex(String indexName) {
            super(indexName);
        }

    }

    public static class GridDBRowValue extends AbstractRowValue<GridDBTables, GridDBColumn, GridDBConstant> {
        GridDBRowValue(GridDBTables tables, Map<GridDBColumn, GridDBConstant> values) {
            super(tables, values);
        }
    }

    public static class GridDBTables extends AbstractTables<GridDBTable, GridDBColumn> {

        public GridDBTables(List<GridDBTable> tables) {
            super(tables);
        }

        public GridDBRowValue getRandomRowValue(GlobalState globalState) throws SQLException {
            SQLConnection con = (SQLConnection) globalState.getConnection();
            // GridDB 不支持 cross join, 故 其查询table一次仅允许一张表
            assert getTables().size() == 1;
            long rowCount = this.getTables().get(0).getNrRows((GridDBGlobalState) globalState);
            GridDBColumn orderByColumn = Randomly.fromList(getColumns());
            // 以OFFSET模拟随机字段
            String randomRow = String.format("SELECT %s FROM %s ORDER BY %s LIMIT 1 OFFSET %d", columnNamesAsString(
                            AbstractTableColumn::getName), tableNamesAsString(), orderByColumn.getName(),
                    globalState.getRandomly().getLong(0, rowCount));
            Map<GridDBColumn, GridDBConstant> values = new HashMap<>();
            try (TSFuzzyStatement s = con.createStatement()) {
                GridDBResultSet randomRowValues = (GridDBResultSet) s.executeQuery(randomRow);
                if (!randomRowValues.hasNext()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    GridDBColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getName());
                    assert columnIndex == i + 1;
                    GridDBConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = GridDBConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INTEGER:
                                value = randomRowValues.getLong(columnIndex);
                                constant = GridDBConstant.createInt32Constant((long) value);
                                break;
                            case TIMESTAMP:
                                Timestamp timestamp = randomRowValues.getTimestamp(columnIndex);
                                value = timestamp.getTime();
                                constant = GridDBConstant.createBigDecimalConstant(new BigDecimal(String.valueOf(value)));
                                break;
                            case LONG:
                                value = randomRowValues.getLong(columnIndex);
                                constant = GridDBConstant.createInt64Constant((long) value);
                                break;
                            case STRING:
                                value = randomRowValues.getString(columnIndex);
                                constant = GridDBConstant.createStringConstant((String) value);
                                break;
                            case FLOAT:
                            case DOUBLE:
                                value = randomRowValues.getDouble(columnIndex);
                                constant = GridDBConstant.createDoubleConstant((double) value);
                                break;
                            default:
                                throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new GridDBRowValue(this, values);
            }
        }

    }

    public GridDBTables getRandomTableNonEmptyTables() {
        // 查询table一次仅允许一张表
        return new GridDBTables(Randomly.nonEmptySubset(getDatabaseTables(), 1));
    }
}
