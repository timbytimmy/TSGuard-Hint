package com.fuzzy.prometheus.resultSet;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.SQLException;

@Data
public class PrometheusResultSet extends DBValResultSet {

    private Object jsonResult;
    private Object curRowRecord;
    private PrometheusRequestType prometheusRequestType;

    public PrometheusResultSet(String db, String table, String jsonResult) {
        super(db, table);
        JSONObject jsonObject = JSONObject.parseObject(jsonResult);
        this.jsonResult = jsonObject.get("data");
    }

    public PrometheusResultSet(String jsonResult, PrometheusRequestType prometheusRequestType) {
        JSONObject jsonObject = JSONObject.parseObject(jsonResult);
        this.jsonResult = jsonObject.get("data");
        this.prometheusRequestType = prometheusRequestType;
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
            cursor++;
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
        // TODO 暂仅支持捞取第一批数据 -> 即精确查询
        JSONArray values = ((JSONObject) jsonResult).getJSONArray("result").getJSONObject(0)
                .getJSONArray("value");
        switch (this.prometheusRequestType) {
            case INSTANT_QUERY:
                return values.getString(columnIndex);
            case RANGE_QUERY:
                return values.getJSONArray(columnIndex).getString(0);
            default:
                throw new SQLException(String.format("prometheusRequestType not support! val:%s", prometheusRequestType));
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return Integer.valueOf(getString(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return Long.parseLong(getString(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return Double.parseDouble(getString(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return new BigDecimal(getString(columnIndex));
    }

//    public Timestamp getTimestamp() throws SQLException {
//        return new Timestamp(getCurrentValue().getTimestamp());
//    }

}
