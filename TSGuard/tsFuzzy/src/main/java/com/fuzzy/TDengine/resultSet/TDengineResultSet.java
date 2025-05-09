package com.fuzzy.TDengine.resultSet;

import com.benchmark.entity.DBValResultSet;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class TDengineResultSet extends DBValResultSet {

    private ResultSet resultSet;
    private TDengineRowRecord curRowRecord;

    public TDengineResultSet(String db, String table, ResultSet resultSet) {
        super(db, table);
        this.resultSet = resultSet;
    }

    public TDengineResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() throws SQLException {
        try {
            return this.resultSet.next();
            // TODO
//            this.curRowRecord = null;
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

    public TDengineRowRecord get(int rowIndex) throws SQLException {
        try {
            // TODO
            resultSet.beforeFirst();
            int i = 0;
            while (resultSet.next() && i++ == rowIndex) {
                return getCurrentValue();
            }
            throw new SQLException("row Index 超出索引范围");
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public TDengineRowRecord getCurrentValue() throws SQLException {
        if (curRowRecord != null) return curRowRecord;
        else throw new SQLException("curRowRecord is null!");
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

//    public Timestamp getTimestamp() throws SQLException {
//        return new Timestamp(getCurrentValue().getTimestamp());
//    }

    public Long getTimestamp() throws SQLException {
        return getCurrentValue().getTimestamp();
    }

    // 针对TDengine返回数据集进行封装
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
