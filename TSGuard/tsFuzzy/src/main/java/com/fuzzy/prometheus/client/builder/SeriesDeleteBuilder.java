package com.fuzzy.prometheus.client.builder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class SeriesDeleteBuilder implements QueryBuilder {
    private static final String TARGET_URI = "/api/v1/admin/tsdb/delete_series?";
    private static final String TARGET_URI_PATTERN_SUFFIX = "#{selector}&start=#{start}&end=#{end}";

    private static final String START_TIME_EPOCH_TIME = "start";
    private static final String END_TIME_EPOCH_TIME = "end";
    private static final String QUERY_STRING = "selector";

    private String serverUrl;
    private Map<String, Object> params = new HashMap<String, Object>();

    public SeriesDeleteBuilder(String serverUrl) {
        this.serverUrl = serverUrl + TARGET_URI;
    }

    public SeriesDeleteBuilder withSelector(String selector) {
        try {
            params.put(QUERY_STRING, URLEncoder.encode(selector, "UTF-8").replaceAll("%3D", "=").replaceAll("%26", "&"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return this;
    }

    public SeriesDeleteBuilder withStartEpochTime(long startTime) {
        params.put(START_TIME_EPOCH_TIME, startTime);
        return this;
    }

    public SeriesDeleteBuilder withEndEpochTime(long endTime) {
        params.put(END_TIME_EPOCH_TIME, endTime);
        return this;
    }

    public URI build() {
        return URI.create(Utils.namedFormat(serverUrl + Utils.namedFormat(TARGET_URI_PATTERN_SUFFIX, params), params));
    }

    private boolean validate() {
        return true;
    }
}
