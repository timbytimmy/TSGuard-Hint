package com.fuzzy.prometheus.apiEntry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusRequestParam {
    // 请求参数类型
    private PrometheusRequestType type;
    // 参数体
    private String body;
}
