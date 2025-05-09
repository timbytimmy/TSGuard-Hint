package com.fuzzy.influxdb.resultSet;

import com.benchmark.entity.DBValResultSet;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.ObjectUtils;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Data
public class InfluxDBResultSet extends DBValResultSet {

    private final String statementId;
    private final List<InfluxDBSeries> seriesList;

    public InfluxDBResultSet() {
        this.statementId = "";
        this.seriesList = new ArrayList<>();
    }

    public InfluxDBResultSet(String db, String table, String statementId, List<InfluxDBSeries> seriesList) {
        super(db, table);
        this.statementId = statementId;
        this.seriesList = seriesList;
    }

    public InfluxDBResultSet(String statementId, List<InfluxDBSeries> seriesList) {
        this.statementId = statementId;
        this.seriesList = seriesList;
    }

    @Override
    protected boolean hasValue() {
        return !ObjectUtils.isEmpty(seriesList) && cursor < seriesList.size();
    }

    @Override
    public boolean hasNext() {
        ++cursor;
        return this.hasValue();
    }

    @Override
    public void resetCursor() {
        cursor = -1;
    }

    public InfluxDBSeries get(int rowIndex) {
        return this.seriesList.get(rowIndex);
    }

    public InfluxDBSeries getCurrentValue() {
        if (this.hasValue()) return seriesList.get(cursor);
        else return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (StringUtils.isBlank(columnLabel)) {
            throw new SQLException("Non-empty column label is required");
        }

        // 扁平化多条序列索引
        int resultIndex = 0;
        for (InfluxDBSeries influxDBSeries : seriesList) {
            for (int columnIndex = 0; columnIndex < influxDBSeries.getColumns().size(); columnIndex++) {
                if (columnLabel.equalsIgnoreCase(influxDBSeries.getColumns().get(columnIndex)))
                    return resultIndex + columnIndex;
            }
            throw new SQLException(String.format("Column [%s] does not exist in %d columns",
                    columnLabel, influxDBSeries.getColumns().size()));
        }

        throw new SQLException(String.format("Column table [%s] does not exist in %d Series",
                columnLabel, this.seriesList.size()));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        for (InfluxDBSeries influxDBSeries : seriesList) {
            if (columnIndex >= influxDBSeries.getColumns().size()) {
                columnIndex -= influxDBSeries.getColumns().size();
                continue;
            }
            // TODO get(0) -> limit 1
            assert !ObjectUtils.isEmpty(influxDBSeries.getValues())
                    && !ObjectUtils.isEmpty(influxDBSeries.getValues().get(0));
            return influxDBSeries.getValues().get(0).get(columnIndex);
        }
        throw new SQLException(String.format("Index[%d] out of bounds", columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return Integer.parseInt(getString(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return Long.parseLong(getString(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return Float.parseFloat(getString(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return Double.parseDouble(getString(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return BigDecimal.valueOf(getLong(columnIndex), scale);
    }

}
