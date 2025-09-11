package com.benchmark.commonClass;

import com.benchmark.entity.DBValResultSet;

import java.sql.SQLException;

public interface TSFuzzyStatement extends AutoCloseable {

    void close() throws SQLException;
    void execute(String body) throws SQLException;
    DBValResultSet executeQuery(String sql) throws SQLException;

}
