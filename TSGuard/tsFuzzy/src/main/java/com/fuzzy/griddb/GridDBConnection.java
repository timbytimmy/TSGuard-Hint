package com.fuzzy.griddb;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class GridDBConnection implements TSFuzzyConnection {

    private Connection conn;

    public GridDBConnection(String host, int port, String userName, String password)
            throws SQLException {
        String jdbcUrl = String.format("jdbc:gs://%s:%d/%s/%s", host, port, GridDBConstantString.CLUSTER_NAME.getName(),
                GridDBConstantString.DATABASE_NAME.getName());
        this.conn = DriverManager.getConnection(jdbcUrl, userName, password);
    }

    @Override
    public GridDBStatement createStatement() throws SQLException {
        return new GridDBStatement(conn.createStatement());
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
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
        return "version 5.7.0";
    }
}
