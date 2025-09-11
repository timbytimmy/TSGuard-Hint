package com.fuzzy.TDengine.resultSet;

import lombok.Data;

import java.util.Map;

@Data
public class TDengineRowRecord {

    private Long timestamp;
    private Map<String, String> fields;

    public TDengineRowRecord(Map<String, String> fields) {
        this.fields = fields;
    }

    public TDengineRowRecord(Map<String, String> fields, long timestamp) {
        this.fields = fields;
        this.timestamp = timestamp;
    }

}
