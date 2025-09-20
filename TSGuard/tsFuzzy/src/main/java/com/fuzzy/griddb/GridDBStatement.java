package com.fuzzy.griddb;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.griddb.resultset.GridDBResultSet;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class GridDBStatement implements TSFuzzyStatement {
    private Statement statement;

    public GridDBStatement(Statement statement) {
        this.statement = statement;
    }

    @Override
    public void close() throws SQLException {
        try {
            this.statement.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void execute(String sql) throws SQLException {
        try {
            statement.execute(sql);
        } catch (Exception e) {
            log.error("执行SQL查询失败, e:", e);
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public DBValResultSet executeQuery(String sql) throws SQLException {
        try {
            return new GridDBResultSet(statement.executeQuery(sql));
        } catch (SQLException e) {
//            log.error("执行SQL查询失败, sql:{} e:", sql, e);
            throw new SQLException(String.format("执行SQL异常, %s", e.getMessage()));
        }
    }
}
