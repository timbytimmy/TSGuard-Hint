package com.fuzzy.TDengine;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class TDengineConnection implements TSFuzzyConnection {

    private Connection conn;

    public TDengineConnection(String host, int port, String userName, String password) throws SQLException {
        String jdbcUrl = String.format("jdbc:TAOS-RS://%s:%d/?user=%s&password=%s", host, port, userName, password);
        this.conn = DriverManager.getConnection(jdbcUrl);
    }

    @Override
    public TDengineStatement createStatement() throws SQLException {
        return new TDengineStatement(conn.createStatement());
    }

    @Override
    public TSFuzzyStatement prepareStatement(String sql) throws SQLException {
        log.warn("prepareStatement is not ready.");
        return null;
    }

    @Override
    public void close() throws SQLException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        return "version 3.0.5.0";
    }
}
