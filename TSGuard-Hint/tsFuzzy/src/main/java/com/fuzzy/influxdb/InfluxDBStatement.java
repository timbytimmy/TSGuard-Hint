package com.fuzzy.influxdb;

import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.benchmark.influxdb.db.InfluxdbDBApiEntry;
import com.benchmark.util.HttpClientUtils;
import com.benchmark.util.HttpRequestEnum;
import com.fuzzy.influxdb.resultSet.InfluxDBResultSet;
import com.fuzzy.influxdb.util.InfluxDBParseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class InfluxDBStatement implements TSFuzzyStatement {
    private InfluxdbDBApiEntry apiEntry;
    private static final String CODE = "code";
    private static final String INVALID_CODE = "invalid";
    private static final String ERROR_RESP = "\"error\":";
    private static final String UNPROCESSABLE_ENTITY_CODE = "unprocessable entity";
    private static final String MESSAGE = "message";

    public InfluxDBStatement(InfluxdbDBApiEntry apiEntry) {
        this.apiEntry = apiEntry;
    }

    @Override
    public void close() throws SQLException {
        // this.apiEntry.close();
    }

    @Override
    public void execute(String query) throws SQLException {
        if (query.startsWith("q=")) executeBySql(query);
        else executeByApiV2(query);
    }

    @Override
    public DBValResultSet executeQuery(String query) throws SQLException {
        if (query.startsWith("q=")) return executeQueryBySql(query);
        else return executeQueryByApiV2(query);
    }

    public String executeManagerApi(String url, String data, HttpRequestEnum requestType) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Token " + String.valueOf(apiEntry.getToken()));
        headers.put("Content-type", "application/json");
        headers.put("Accept", "application/json");
        return HttpClientUtils.sendRequest(url, data, headers, requestType);
    }

    // execute
    private void executeByApiV2(String body) throws SQLException {
        try {
            String url = String.format("http://%s:%d/api/v2/write?org=%s&bucket=%s&precision=%s", apiEntry.getHost(),
                    apiEntry.getPort(), apiEntry.getOrgId(), apiEntry.getBucket(), apiEntry.getPrecision());
            Map<String, String> headers = new HashMap<>();
            headers.put("Authorization", "Token " + String.valueOf(apiEntry.getToken()));
            headers.put("Content-type", "text/plain; charset=utf-8");
            headers.put("Accept", "application/json");
            String response = HttpClientUtils.sendRequest(url, body, headers, HttpRequestEnum.POST);
            if (!StringUtils.isBlank(response)) {
                JSONObject result = JSONObject.parseObject(response);
                String code = result.getString(CODE);
                if (!StringUtils.isBlank(code))
                    throw new Exception(String.format("sql:%s \n e: %s", body, result.getString(MESSAGE)));
            }
        } catch (Exception e) {
            log.error("执行SQL失败, e:", e);
            throw new SQLException(e.getMessage());
        }
    }

    private void executeBySql(String sql) throws SQLException {
        String url = String.format("http://%s:%d/query?db=%s", apiEntry.getHost(), apiEntry.getPort(),
                apiEntry.getBucket());
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Token " + String.valueOf(apiEntry.getToken()));
        headers.put("Content-type", "application/x-www-form-urlencoded");

        try {
            sql = URLEncoder.encode(sql, "UTF-8");
            sql = sql.replace("%3D", "=").replace("%26", "&");
        } catch (Exception e) {
            log.error("编码SQL失败, e:", e);
            throw new SQLException(String.format("编码SQL失败, e:%s", e));
        }
        String response = HttpClientUtils.sendRequest(url, sql, headers, HttpRequestEnum.POST);
        if (!StringUtils.isBlank(response)) {
            JSONObject result = JSONObject.parseObject(response);
            String code = result.getString(CODE);
            if (!StringUtils.isBlank(code))
                throw new SQLException(result.getString(MESSAGE));
            if (response.contains(ERROR_RESP)) throw new SQLException(response);
        }
    }

    // executeQuery
    private DBValResultSet executeQueryByApiV2(String body) throws SQLException {
        log.info("executeQueryByApiV2...");
        return new InfluxDBResultSet(null, null);
    }

    private DBValResultSet executeQueryBySql(String sql) throws SQLException {
        InfluxDBResultSet influxDBResultSet = new InfluxDBResultSet();
        String url = String.format("http://%s:%d/query?db=%s", apiEntry.getHost(), apiEntry.getPort(),
                apiEntry.getBucket());
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Token " + String.valueOf(apiEntry.getToken()));
        headers.put("Content-type", "application/x-www-form-urlencoded");

        try {
            sql = URLEncoder.encode(sql, "UTF-8");
            sql = sql.replace("%3D", "=").replace("%26", "&");
        } catch (Exception e) {
            log.error("编码SQL失败, e:", e);
            throw new SQLException(String.format("编码SQL失败, e:%s", e));
        }
        String response = HttpClientUtils.sendRequest(url, sql, headers, HttpRequestEnum.POST);
        if (!StringUtils.isBlank(response)) {
            JSONObject result = JSONObject.parseObject(response);
            String code = result.getString(CODE);
            if (!StringUtils.isBlank(code))
                throw new SQLException(result.getString(MESSAGE));
            if (response.contains(ERROR_RESP)) throw new SQLException(response);
            influxDBResultSet = InfluxDBParseUtil.parseInfluxQLQueryResult(JSONObject.toJSONString(result));
        }
        return influxDBResultSet;
    }
}
