package com.fuzzy.prometheus.apiEntry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusRequestParam {
    private PrometheusRequestType type;
}
