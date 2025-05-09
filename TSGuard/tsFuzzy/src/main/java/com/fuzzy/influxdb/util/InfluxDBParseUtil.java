package com.fuzzy.influxdb.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fuzzy.influxdb.resultSet.InfluxDBResultSet;
import com.fuzzy.influxdb.resultSet.InfluxDBSeries;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBParseUtil {

    public static InfluxDBResultSet parseInfluxQLQueryResult(String jsonResult) {
        List<InfluxDBSeries> influxDBSeriesList = new ArrayList<>();
        JSONObject schemaResult = (JSONObject) JSONObject.parseObject(jsonResult).getJSONArray("results").get(0);
        String statementId = schemaResult.getString("statement_id");
        JSONArray seriesArray = schemaResult.getJSONArray("series");
        if (!ObjectUtils.isEmpty(seriesArray)) {
            seriesArray.forEach(series -> {
                JSONObject seriesObject = (JSONObject) series;
                InfluxDBSeries influxDBSeries = new InfluxDBSeries();
                influxDBSeries.setName(seriesObject.getString("name"));
                influxDBSeries.setColumns(seriesObject.getJSONArray("columns").toJavaList(String.class));
                List<List<String>> seriesValues = new ArrayList<>();
                seriesObject.getJSONArray("values").forEach(valueList -> {
                    List<String> stringList = new ArrayList<>();
                    JSONArray valueArray = (JSONArray) valueList;
                    for (Object value : valueArray) {
                        // 全部以String存储, 取出时按照各自类型进行转换
                        stringList.add(String.valueOf(value));
                    }
                    seriesValues.add(stringList);
                });
                influxDBSeries.setValues(seriesValues);
                influxDBSeriesList.add(influxDBSeries);
            });
        }
        InfluxDBResultSet influxDBResultSet = new InfluxDBResultSet(statementId, influxDBSeriesList);
        return influxDBResultSet;
    }

}
