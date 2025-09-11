package com.fuzzy.griddb.resultset;

import com.benchmark.entity.DBValResultSet;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

@Data
public class GridDBResultSet extends DBValResultSet {

    private ResultSet resultSet;

    public GridDBResultSet(String db, String table, ResultSet resultSet) {
        super(db, table);
        this.resultSet = resultSet;
    }

    public GridDBResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() throws SQLException {
        try {
            return this.resultSet.next();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            this.resultSet.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return resultSet.wasNull();
    }

    /**
     * @param columnLabel
     * @return int
     * @description 查找除时间戳外的Column
     * @dateTime 2024/4/24 15:57
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        try {
            return resultSet.findColumn(columnLabel);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSet.getMetaData();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return resultSet.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return resultSet.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return resultSet.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return resultSet.getDouble(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Timestamp timestampUTC = resultSet.getTimestamp(columnIndex);
        TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
        return new Timestamp(timestampUTC.getTime() - timeZone.getOffset(timestampUTC.getTime()));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return resultSet.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return resultSet.getBoolean(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return resultSet.getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return resultSet.getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return resultSet.getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return resultSet.getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return resultSet.getDouble(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return resultSet.getBigDecimal(columnLabel, scale);
    }

//    public Long getTimestamp() throws SQLException {
//        return getCurrentValue().getTimestamp();
//    }

    // 针对GridDB返回数据集进行封装
    public Map<String, Integer> getColumnOrdinalMap() throws SQLException {
        try {
            ResultSetMetaData metaData = this.resultSet.getMetaData();
            Map<String, Integer> columnOrdinalMap = new HashMap<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                columnOrdinalMap.put(metaData.getColumnName(i), i);
            }
            return columnOrdinalMap;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public List<String> getColumnNames() throws SQLException {
        try {
            ResultSetMetaData metaData = this.resultSet.getMetaData();
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                columnNames.add(metaData.getColumnName(i));
            }
            return columnNames;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public List<String> getColumnTypes() throws SQLException {
        try {
            ResultSetMetaData metaData = this.resultSet.getMetaData();
            List<String> typeNames = new ArrayList<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                typeNames.add(metaData.getColumnTypeName(i));
            }
            return typeNames;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

}
