package com.fuzzy.iotdb;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.iotdb.db.IotdbDBApiEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.sql.SQLException;

@Slf4j
public class IotDBConnection implements TSFuzzyConnection {

    private IotdbDBApiEntry apiEntry;

    public IotDBConnection(String host, int port, String userName, String password) {
        this.apiEntry = new IotdbDBApiEntry(host, port, userName, password);
    }

    @Override
    public IotDBStatement createStatement() throws SQLException {
        return new IotDBStatement(apiEntry);
    }

    @Override
    public TSFuzzyStatement prepareStatement(String sql) throws SQLException {
        log.warn("prepareStatement is not ready.");
        return null;
    }

    @Override
    public void close() throws SQLException {
        try {
            apiEntry.close();
        } catch (IoTDBConnectionException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    // 测试联通性
    public boolean isClosed() throws SQLException {
        return !apiEntry.isConnected();
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        return "version 1.3.0";
    }
}
