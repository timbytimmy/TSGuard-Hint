package com.fuzzy.prometheus;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.prometheus.apiEntry.PrometheusApiEntry;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.sql.SQLException;

@Slf4j
public class PrometheusConnection implements TSFuzzyConnection {

    private PrometheusApiEntry apiEntry;

    public PrometheusConnection(String host, int port) {
        this.apiEntry = new PrometheusApiEntry(host, port);
    }

    @Override
    public PrometheusStatement createStatement() throws SQLException {
        return new PrometheusStatement(apiEntry);
    }

    @Override
    public TSFuzzyStatement prepareStatement(String sql) throws SQLException {
        log.warn("prepareStatement is not ready.");
        return null;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    // 测试联通性
    public boolean isClosed() throws SQLException {
        URI url = URI.create(this.apiEntry.getTargetServer() + "/-/healthy");
        String result = this.apiEntry.executeGetRequest(url);
        return !result.equalsIgnoreCase("Prometheus Server is Healthy.\n");
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        return "version 2.51.2";
    }
}
