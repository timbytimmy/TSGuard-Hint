package com.fuzzy.prometheus.apiEntry;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PrometheusQueryParam extends PrometheusRequestParam {
    private String requestBody;
    private Long start;
    private Long end;
    private Long step;
    private Long limit;

    public PrometheusQueryParam(PrometheusRequestType type, String requestBody) {
        super(type);
        this.requestBody = requestBody;
    }

    public PrometheusQueryParam(PrometheusRequestType type, String requestBody, long start) {
        super(type);
        this.requestBody = requestBody;
        this.start = start;
    }

    public PrometheusQueryParam(PrometheusRequestType type) {
        super(type);
    }
}
