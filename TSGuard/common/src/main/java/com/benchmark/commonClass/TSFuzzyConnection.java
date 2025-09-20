package com.benchmark.commonClass;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public interface TSFuzzyConnection extends AutoCloseable {

    TSFuzzyStatement createStatement() throws SQLException;
    TSFuzzyStatement prepareStatement(String sql) throws SQLException;
    void close() throws SQLException;
    boolean isClosed() throws SQLException;
    String getDatabaseVersion() throws SQLException;
    default DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

}
