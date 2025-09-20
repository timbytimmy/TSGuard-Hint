package com.fuzzy;

import com.benchmark.commonClass.TSFuzzyConnection;
import com.benchmark.commonClass.TSFuzzyStatement;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class SQLConnection implements TSFuzzyDBConnection {

    private final TSFuzzyConnection connection;

    public SQLConnection(TSFuzzyConnection connection) {
        this.connection = connection;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        return connection.getDatabaseVersion();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    public TSFuzzyStatement prepareStatement(String arg) throws SQLException {
        return connection.prepareStatement(arg);
    }

    public TSFuzzyStatement createStatement() throws SQLException {
        return connection.createStatement();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }
}
