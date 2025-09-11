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
    private String metricName;
    @JSONField(name = "database")
    private String database;
    @JSONField(name = "table")
    private String table;
    @JSONField(name = "exported_job")
    private String column;

    public PrometheusColumn transToColumn() {
        PrometheusDataType dataType;
        if (metricName.endsWith("_total")) dataType = PrometheusDataType.COUNTER;
        else dataType = PrometheusDataType.GAUGE;
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
