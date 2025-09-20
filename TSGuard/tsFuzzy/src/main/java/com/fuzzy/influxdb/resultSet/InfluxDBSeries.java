package com.fuzzy.influxdb.resultSet;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Data
@Slf4j
public class InfluxDBSeries {

    private String name;
    private List<String> columns;
    private List<List<String>> values;

    // 针对查询MEASUREMENTS设置
    public List<String> getAllValues() {
        return values.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equalsIgnoreCase(columnName)) return i;
        }
        return -1;
    }

    public List<Long> getTimestampForSeries(Map<String, String> tagList) {
        List<List<String>> filterResult = values;
        for (Entry<String, String> tagEntry : tagList.entrySet()) {
            filterResult = filterResult.stream().filter(row -> {
                boolean success = false;
                try {
                    success = row.get(getColumnIndex(tagEntry.getKey())).equalsIgnoreCase(tagEntry.getValue());
                } catch (Exception e) {
                    log.warn("列名索引不存在, column:{}", tagEntry.getKey());
                }
                return success;
            }).collect(Collectors.toList());
        }
        return filterResult.stream().map(
                        row -> Long.parseLong(row.get(getColumnIndex("time"))))
                .collect(Collectors.toList());
    }

}
