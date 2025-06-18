package com.fuzzy.prometheus.apiEntry.entity;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.annotation.JSONField;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusColumn;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import lombok.Data;

import java.util.Objects;

@Data
public class PrometheusSeriesResultItem {
    @JSONField(name = "__name__")
    private String database;
    @JSONField(name = "table")
    private String table;
    @JSONField(name = "timeSeries")
    private String column;

    public PrometheusColumn transToColumn() {
        // TODO 列类型的确定，暂定测试 GAUGE 类型
        PrometheusDataType dataType = PrometheusDataType.GAUGE;
//        if (database.endsWith("_total")) dataType = PrometheusDataType.COUNTER;
//        else dataType = PrometheusDataType.GAUGE;
        return new PrometheusColumn(column, false, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrometheusSeriesResultItem series = (PrometheusSeriesResultItem) o;
        return ObjectUtil.equals(this.database, series.database) && ObjectUtil.equals(this.table, series.table)
                && ObjectUtil.equals(this.column, series.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, column);
    }
}
