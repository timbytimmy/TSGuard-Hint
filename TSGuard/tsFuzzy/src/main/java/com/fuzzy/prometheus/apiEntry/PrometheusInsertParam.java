package com.fuzzy.prometheus.apiEntry;

import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import lombok.Data;

import java.util.Map;

@Data
public class PrometheusInsertParam extends PrometheusRequestParam {
    // <metricName, Collector>
    Map<String, CollectorAttribute> collectorList;
}
