package com.fuzzy.influxdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.schema.AbstractTableColumn;
import com.fuzzy.influxdb.InfluxDBGlobalState;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBRowValue;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTable;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBTables;
import com.fuzzy.influxdb.InfluxDBVisitor;
import com.fuzzy.influxdb.ast.InfluxDBColumnReference;
import com.fuzzy.influxdb.ast.InfluxDBConstant;
import com.fuzzy.influxdb.ast.InfluxDBExpression;
import com.fuzzy.influxdb.ast.InfluxDBUnaryNotPrefixOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBShowStatementGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final InfluxDBGlobalState globalState;
    private ExpectedErrors errors = new ExpectedErrors();

    public InfluxDBShowStatementGenerator(InfluxDBGlobalState globalState) {
        // 针对SHOW语句，语法不符合问题
        errors.add("invalid tag comparison operator");
        errors.add("left side of '=' must be a tag key");
        errors.add("left side of '!=' must be a tag key");
        errors.add("expression too complex for metaquery");
        errors.add("failed to parse query");

        this.globalState = globalState;
        sb.append("q=");
    }

    public enum ShowFieldType {
        Field,
        Tag,
        All
    }

    public enum ShowStatementType {
        SHOW_FIELD_KEYS,
        SHOW_FIELD_KEY_CARDINALITY,
        SHOW_MEASUREMENTS,
        SHOW_SERIES,
        SHOW_SERIES_CARDINALITY,
        SHOW_TAG_KEYS,
        SHOW_TAG_KEY_CARDINALITY,
        SHOW_TAG_VALUES,
        SHOW_TAG_VALUES_CARDINALITY,
    }

    public static SQLQueryAdapter generateShowStatement(InfluxDBGlobalState globalState, ShowStatementType type)
            throws Exception {
        switch (type) {
            case SHOW_FIELD_KEYS:
                return new InfluxDBShowStatementGenerator(globalState).generateShowFieldKeys();
            case SHOW_FIELD_KEY_CARDINALITY:
                return new InfluxDBShowStatementGenerator(globalState).generateShowFieldKeyCardinality();
            case SHOW_MEASUREMENTS:
                return new InfluxDBShowStatementGenerator(globalState).generateShowMeasurements();
            case SHOW_SERIES:
                return new InfluxDBShowStatementGenerator(globalState).generateShowSeries();
            case SHOW_SERIES_CARDINALITY:
                return new InfluxDBShowStatementGenerator(globalState).generateShowSeriesCardinality();
            case SHOW_TAG_KEYS:
                return new InfluxDBShowStatementGenerator(globalState).generateShowTagKeys();
            case SHOW_TAG_KEY_CARDINALITY:
                return new InfluxDBShowStatementGenerator(globalState).generateShowTagKeyCardinality();
            case SHOW_TAG_VALUES:
                return new InfluxDBShowStatementGenerator(globalState).generateShowTagValues();
            case SHOW_TAG_VALUES_CARDINALITY:
                return new InfluxDBShowStatementGenerator(globalState).generateShowTagValueCardinality();
            default:
                throw new AssertionError();
        }
    }

    private SQLQueryAdapter generateShowFieldKeyCardinality() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        if (Randomly.getBoolean()) sb.append("SHOW FIELD KEY CARDINALITY");
        else sb.append("SHOW FIELD KEY EXACT CARDINALITY");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWhereClause(randomTable, ShowFieldType.Field);
        // TODO bug
        // generateAndAppendGroupByClause(randomTable.getColumns());
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowFieldKeys() {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("SHOW FIELD KEYS");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowMeasurements() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("SHOW MEASUREMENTS");

        generateAndAppendOnClause(randomTable);
        generateAndAppendWithMeasurementClause(randomTable);
        generateAndAppendWhereClause(randomTable, ShowFieldType.All);
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowSeries() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("SHOW SERIES");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWhereClause(randomTable, ShowFieldType.All);
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowSeriesCardinality() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("SHOW SERIES EXACT CARDINALITY");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWhereClause(randomTable, ShowFieldType.All);
        generateAndAppendGroupByClause(randomTable.getColumns());
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowTagKeys() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("SHOW TAG KEYS");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWhereClause(randomTable, ShowFieldType.Tag);
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowTagKeyCardinality() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        if (Randomly.getBoolean()) sb.append("SHOW TAG KEY CARDINALITY");
        else sb.append("SHOW TAG KEY EXACT CARDINALITY");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWhereClause(randomTable, ShowFieldType.Tag);
        generateAndAppendGroupByClause(randomTable.getColumns());
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowTagValues() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append("SHOW TAG VALUES");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWithTagClause(randomTable.getColumns(), true);
        generateAndAppendWhereClause(randomTable, ShowFieldType.Tag);
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private SQLQueryAdapter generateShowTagValueCardinality() throws Exception {
        InfluxDBTable randomTable = globalState.getSchema().getRandomTable();
        if (Randomly.getBoolean()) sb.append("SHOW TAG VALUES CARDINALITY");
        else sb.append("SHOW TAG VALUES EXACT CARDINALITY");

        boolean hasOnClause = generateAndAppendOnClause(randomTable);
        generateAndAppendFromClause(randomTable, hasOnClause);
        generateAndAppendWithTagClause(randomTable.getColumns(), true);
        generateAndAppendWhereClause(randomTable, ShowFieldType.Tag);
        generateAndAppendGroupByClause(randomTable.getColumns());
        generateAndAppendLimitClause();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    // common dropRandomTable functions
    private boolean generateAndAppendOnClause(InfluxDBTable table) {
        if (Randomly.getBoolean()) {
            sb.append(" ON ");
            sb.append(table.getDatabaseName());
            return true;
        }
        return false;
    }

    private void generateAndAppendFromClause(InfluxDBTable table, boolean hasOnClause) {
        if (Randomly.getBoolean()) {
            sb.append(" FROM ");
            if (hasOnClause && Randomly.getBoolean()) sb.append(table.getName());
            else sb.append(table.getFullName());
        }
    }

    private void generateAndAppendWhereClause(InfluxDBTable table, ShowFieldType showFieldType) throws Exception {
        if (Randomly.getBoolean()) {
            List<InfluxDBColumn> columns = table.getColumns();
            switch (showFieldType) {
                case Field:
                    columns = columns.stream().filter(column -> !column.isTag()).collect(Collectors.toList());
                    break;
                case Tag:
                    columns = columns.stream().filter(AbstractTableColumn::isTag).collect(Collectors.toList());
                    break;
                case All:
                    break;
                default:
                    throw new AssertionError();
            }

            InfluxDBRowValue pivotRow = new InfluxDBTables(new ArrayList<>(Collections.singletonList(table)))
                    .getRandomRowValue(globalState);
            InfluxDBExpression expression = new InfluxDBExpressionGenerator(globalState).setRowVal(pivotRow)
                    .setColumns(columns).generateExpression();

            InfluxDBConstant expectedValue = expression.getExpectedValue();
            InfluxDBExpression whereClause;
            if (expectedValue.isNull()) {
                throw new AssertionError("InfluxDB不支持Null");
            } else if (expectedValue.asBooleanNotNull()) {
                whereClause = expression;
            } else {
                whereClause = InfluxDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(expression);
            }
            sb.append(" WHERE ");
            sb.append(InfluxDBVisitor.asString(whereClause));
        }
    }

    private void generateAndAppendGroupByClause(List<InfluxDBColumn> columns) {
        if (Randomly.getBoolean()) {
            sb.append(" GROUP BY ");
            List<InfluxDBExpression> groupByClause = columns.stream().map(c -> InfluxDBColumnReference.create(c,
                            InfluxDBConstant.createDoubleQuotesStringConstant("Column Reference no value")))
                    .collect(Collectors.toList());
            for (int i = 0; i < groupByClause.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(InfluxDBVisitor.asString(groupByClause.get(i)));
            }
        }
    }

    private void generateAndAppendLimitClause() {
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(InfluxDBVisitor.asString(InfluxDBConstant.createIntConstant(Integer.MAX_VALUE, true, false)));
            generateAndAppendOffsetClause();
        }
    }

    private void generateAndAppendOffsetClause() {
        if (Randomly.getBoolean()) {
            sb.append(" OFFSET ");
            sb.append(InfluxDBVisitor.asString(InfluxDBConstant.createIntConstantNotAsBoolean(0)));
        }
    }

    private void generateAndAppendWithTagClause(List<InfluxDBColumn> columns, boolean forceUseWith) {
        if (Randomly.getBoolean() || forceUseWith) {
            InfluxDBColumn randomColumn = Randomly.fromList(columns.stream()
                    .filter(AbstractTableColumn::isTag).collect(Collectors.toList()));
            sb.append(" WITH KEY ");
            sb.append(String.format("= \"%s\"", randomColumn.getName()));
        }
    }

    private void generateAndAppendWithMeasurementClause(InfluxDBTable table) {
        if (Randomly.getBoolean()) {
            sb.append(" WITH MEASUREMENT ");
            sb.append(String.format("= \"%s\"", table.getDatabaseName()));
        }
    }

}
