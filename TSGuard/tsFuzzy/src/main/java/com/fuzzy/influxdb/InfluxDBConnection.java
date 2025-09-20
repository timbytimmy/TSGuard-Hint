package com.fuzzy.influxdb;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.influxdb.db.InfluxdbDBApiEntry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class InfluxDBConnection implements TSFuzzyConnection {

    private InfluxdbDBApiEntry apiEntry;

    public InfluxDBConnection(String host, int port, String tokenString,
                              String orgId, String bucket, String precision) throws SQLException {
        try {
            this.apiEntry = new InfluxdbDBApiEntry(host, port, tokenString, orgId, bucket, precision);
        } catch (IOException e) {
            throw new SQLException(String.format("connection error, e:%s", e.getMessage()));
        }
    }

    @Override
    public InfluxDBStatement createStatement() throws SQLException {
        return new InfluxDBStatement(apiEntry);
    }

    @Override
    public TSFuzzyStatement prepareStatement(String sql) throws SQLException {
        log.warn("prepareStatement is not ready.");
        return null;
    }

    @Override
    public void close() throws SQLException {
        apiEntry.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !apiEntry.isConnected();
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        return "OSS 2.7";
    }
}
