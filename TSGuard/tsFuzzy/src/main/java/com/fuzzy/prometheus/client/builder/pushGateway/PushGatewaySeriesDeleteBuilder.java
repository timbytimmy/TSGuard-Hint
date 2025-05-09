package com.fuzzy.prometheus.client.builder.pushGateway;

import com.fuzzy.prometheus.client.builder.QueryBuilder;
import com.fuzzy.prometheus.client.builder.Utils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class PushGatewaySeriesDeleteBuilder implements QueryBuilder {
    private static final String TARGET_URI = "/metrics/job/#{jobName}";

    private static final String QUERY_STRING = "jobName";

    private String serverUrl;
    private Map<String, Object> params = new HashMap<String, Object>();

    public PushGatewaySeriesDeleteBuilder(String serverUrl) {
        this.serverUrl = serverUrl + TARGET_URI;
    }

    public PushGatewaySeriesDeleteBuilder withJobName(String selector) {
        try {
            params.put(QUERY_STRING, URLEncoder.encode(selector, "UTF-8").replaceAll("%3D", "=").replaceAll("%26", "&"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return this;
    }

    public URI build() {
        return URI.create(Utils.namedFormat(serverUrl, params));
    }

    private boolean validate() {
        return true;
    }
}
