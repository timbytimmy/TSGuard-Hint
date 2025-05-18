package com.fuzzy.prometheus.apiEntry.entity;

import com.fuzzy.Randomly;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusDataType;
import com.fuzzy.prometheus.constant.PrometheusLabelConstant;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Data
public class CollectorAttribute {
    private PrometheusDataType dataType;
    private String metricName;
    private String tableName;
    private List<Double> doubleValues;
    private List<Integer> intValues;
    private List<Long> timestamps;
    // nullable
    private String timeSeriesName;
    private Map<String, String> labels;
    private String help;

    public CollectorAttribute() {
        this.doubleValues = new ArrayList<>();
        this.intValues = new ArrayList<>();
        this.timestamps = new ArrayList<>();
    }

    public Collector transToCollector() {
        List<String> labelValues = new ArrayList<>();
        labelValues.add(this.tableName);
        labelValues.addAll(labels.values());

        switch (this.dataType) {
            case COUNTER:
                Counter counter = Counter.build()
                        .name(this.metricName)
                        .labelNames(PrometheusLabelConstant.TABLE.getLabel())
                        .labelNames(labels.keySet().toArray(new String[0]))
                        .help(this.help)
                        .register();
                // TODO
                counter.labels(labelValues.toArray(new String[0])).inc(intValues.get(0));
                return counter;
            case GAUGE:
                Gauge gauge = Gauge.build()
                        .name(this.metricName)
                        .labelNames(PrometheusLabelConstant.TABLE.getLabel())
                        .labelNames(labels.keySet().toArray(new String[0]))
                        .help(this.help)
                        .register();
                gauge.labels(labelValues.toArray(new String[0])).set(doubleValues.get(0));
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
                this.intValues = Collections.singletonList(randomly.getInteger(1, Integer.MAX_VALUE));
                break;
            case GAUGE:
                this.doubleValues = Collections.singletonList(randomly.getInfiniteDouble());
                break;
            case HISTOGRAM:
            case SUMMARY:
            default:
        }
    }

    public void defaultValue() {
        switch (this.dataType) {
            case COUNTER:
                this.intValues = Collections.singletonList(0);
                break;
            case GAUGE:
                this.doubleValues = Collections.singletonList(0.0);
                break;
            case HISTOGRAM:
            case SUMMARY:
            default:
        }
        this.timestamps = Collections.singletonList(1641024000000L);
    }
}
