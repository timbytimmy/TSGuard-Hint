package com.fuzzy.prometheus.resultSet;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.benchmark.entity.DBValResultSet;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.SQLException;

@Data
public class PrometheusResultSet extends DBValResultSet {

    private Object jsonResult;
    private Object curRowRecord;

    public PrometheusResultSet(String db, String table, String jsonResult) {
        super(db, table);
        JSONObject jsonObject = JSONObject.parseObject(jsonResult);
        this.jsonResult = jsonObject.get("data");
    }

    public PrometheusResultSet(String jsonResult) {
        JSONObject jsonObject = JSONObject.parseObject(jsonResult);
        this.jsonResult = jsonObject.get("data");
    }

    @Override
    protected boolean hasValue() throws SQLException {
        if (jsonResult instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) jsonResult;
            return !jsonArray.isEmpty() && cursor < jsonArray.size();
        } else {
            return !ObjectUtil.isNull(jsonResult) && cursor < 1;
        }
    }

    @Override
    public boolean hasNext() throws SQLException {
        try {
            cursor ++;
            boolean hasValue = hasValue();
            if (!hasValue) return false;
            if (jsonResult instanceof JSONArray) {
                this.curRowRecord = ((JSONArray) jsonResult).get(cursor);
            } else {
                this.curRowRecord = jsonResult;
            }
            return true;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
//        try {
//            this.jsonResult.close();
//        } catch (Exception e) {
//            throw new SQLException(e.getMessage());
//        }
    }

    public String get(int rowIndex) throws SQLException {
//        try {
//            while (jsonResult.hasNext()) {
//
//            }
//            return null;
//        } catch (Exception e) {
//            throw new SQLException(e.getMessage());
//        }
        return null;
    }

    public Object getCurrentValue() throws SQLException {
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
//        try {
//            List<String> columnNames = jsonResult.getColumnNames();
//            for (int i = 0; i < columnNames.size(); i++) {
//                if (columnNames.get(i).equalsIgnoreCase(columnLabel)) return i - 1;
//            }
//            return -1;
//        } catch (Exception e) {
//            throw new SQLException(e.getMessage());
//        }
        return 0;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return null;
//        return getCurrentValue().getFields().get(columnIndex).getStringValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return 0;
//        return getCurrentValue().getFields().get(columnIndex).getIntV();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return 0;
//        return getCurrentValue().getFields().get(columnIndex).getLongV();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
//        return getCurrentValue().getFields().get(columnIndex).getFloatV();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return 0;
//        return getCurrentValue().getFields().get(columnIndex).getDoubleV();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return BigDecimal.valueOf(getLong(columnIndex), scale);
    }

//    public Timestamp getTimestamp() throws SQLException {
//        return new Timestamp(getCurrentValue().getTimestamp());
//    }

}
