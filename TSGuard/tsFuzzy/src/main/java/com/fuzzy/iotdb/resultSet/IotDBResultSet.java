package com.fuzzy.iotdb.resultSet;

import com.benchmark.entity.DBValResultSet;
import com.fuzzy.iotdb.IotDBSchema;
import lombok.Data;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class IotDBResultSet extends DBValResultSet {

    private SessionDataSet sessionDataSet;
    private RowRecord curRowRecord;

    public IotDBResultSet(String db, String table, SessionDataSet sessionDataSet) {
        super(db, table);
        this.sessionDataSet = sessionDataSet;
    }

    public IotDBResultSet(SessionDataSet sessionDataSet) {
        this.sessionDataSet = sessionDataSet;
    }

    @Override
    protected boolean hasValue() throws SQLException {
        try {
            return sessionDataSet.hasNext();
        } catch (StatementExecutionException statementExecutionException) {
            throw new SQLException(statementExecutionException.getMessage());
        } catch (IoTDBConnectionException ioTDBConnectionException) {
            throw new SQLException(ioTDBConnectionException.getMessage());
        }
    }

    @Override
    public boolean hasNext() throws SQLException {
        try {
            boolean hasValue = hasValue();
            if (hasValue) this.curRowRecord = sessionDataSet.next();
            return hasValue;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            this.sessionDataSet.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public RowRecord get(int rowIndex) throws SQLException {
        try {
            while (sessionDataSet.hasNext()) {

            }
            return null;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public RowRecord getCurrentValue() throws SQLException {
        if (curRowRecord != null) return curRowRecord;
        else throw new SQLException("curRowRecord is null!");
    }

    public boolean valueIsNull(int columnIndex) {
        return curRowRecord.getFields().get(columnIndex).getStringValue().equalsIgnoreCase("null");
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
            List<String> columnNames = sessionDataSet.getColumnNames();
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnNames.get(i).equalsIgnoreCase(columnLabel)) return i - 1;
            }
            return -1;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getCurrentValue().getFields().get(columnIndex).getStringValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getCurrentValue().getFields().get(columnIndex).getIntV();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getCurrentValue().getFields().get(columnIndex).getLongV();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getCurrentValue().getFields().get(columnIndex).getFloatV();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getCurrentValue().getFields().get(columnIndex).getDoubleV();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return BigDecimal.valueOf(getLong(columnIndex), scale);
    }

    public Timestamp getTimestamp() throws SQLException {
        return new Timestamp(getCurrentValue().getTimestamp());
    }

    // 针对IotDB返回数据集进行封装
    public Map<String, Integer> getColumnOrdinalMap() throws SQLException {
        try {
            Map<String, Integer> columnOrdinalMap = new HashMap<>();
            for (int i = 0; i < this.sessionDataSet.getColumnNames().size(); i++) {
                columnOrdinalMap.put(this.sessionDataSet.getColumnNames().get(i), i);
            }
            return columnOrdinalMap;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public List<String> getColumnNames() throws SQLException {
        try {
            return this.sessionDataSet.getColumnNames();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public List<String> getColumnTypes() throws SQLException {
        try {
            return this.sessionDataSet.getColumnTypes();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public IotDBSchema.IotDBDataType getColumnType(String columnLabel) {
        List<String> columnNames = sessionDataSet.getColumnNames();
        List<String> columnTypes = sessionDataSet.getColumnTypes();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnLabel.equalsIgnoreCase(columnNames.get(i)))
                return IotDBSchema.IotDBDataType.getDataTypeByName(columnTypes.get(i));
        }
        throw new IllegalArgumentException(String.format("该列无法找到对于的列类型, columnLabel:%s", columnLabel));
    }
}
