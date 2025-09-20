package com.fuzzy.prometheus.apiEntry.entity;

import com.fuzzy.Randomly;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import com.fuzzy.prometheus.constant.PrometheusLabelConstant;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import lombok.Data;

@Data
public class CollectorAttribute {
    private PrometheusDataType dataType;
    private String metricName;
    private String help;
    private String databaseName;
    private String tableName;
    private double doubleValue;
    private int intValue;

    public Collector transToCollector() {
        switch (this.dataType) {
            case COUNTER:
                Counter counter = Counter.build()
                        .name(this.metricName)
                        .labelNames(PrometheusLabelConstant.DATABASE.getLabel(), PrometheusLabelConstant.TABLE.getLabel())
                        .help(this.help)
                        .register();
                counter.labels(this.databaseName, this.tableName).inc(intValue);
                return counter;
            case GAUGE:
                Gauge gauge = Gauge.build()
                        .name(this.metricName)
                        .labelNames(PrometheusLabelConstant.DATABASE.getLabel(), PrometheusLabelConstant.TABLE.getLabel())
                        .help(this.help)
                        .register();
                gauge.labels(this.databaseName, this.tableName).set(this.doubleValue);
                return gauge;
            case HISTOGRAM:
            case SUMMARY:
            default:
                return null;
        }
    }

    public void randomInitValue(Randomly randomly) {
        switch (this.dataType) {
            case COUNTER:
                this.intValue = randomly.getInteger(1, Integer.MAX_VALUE);
                break;
            case GAUGE:
                this.doubleValue = randomly.getInfiniteDouble();
                break;
            case HISTOGRAM:
            case SUMMARY:
            default:
        }
    }
}
