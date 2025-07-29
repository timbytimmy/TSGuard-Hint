package com.fuzzy.prometheus.apiEntry;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PrometheusQueryParam {
    private String requestBody;
    private Long start;
    private Long end;
    private Long step;
    private Long limit;

    public PrometheusQueryParam(String requestBody, long start) {
        this.requestBody = requestBody;
        this.start = start;
    }

    public PrometheusQueryParam(String requestBody) {
        this.requestBody = requestBody;
    }

    public String genPrometheusRequestParam(PrometheusRequestType type) {
        return JSONObject.toJSONString(new PrometheusRequestParam(type, JSONObject.toJSONString(this)));
    }
}
