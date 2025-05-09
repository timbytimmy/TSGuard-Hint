package com.fuzzy.iotdb;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.benchmark.iotdb.db.IotdbDBApiEntry;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class IotDBStatement implements TSFuzzyStatement {
    private IotdbDBApiEntry apiEntry;

    public IotDBStatement(IotdbDBApiEntry apiEntry) {
        this.apiEntry = apiEntry;
    }

    public void open() throws SQLException {
        try {
            this.apiEntry.open();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            this.apiEntry.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void execute(String sql) throws SQLException {
        try {
            if (!apiEntry.isConnected()) open();
            apiEntry.executeNonQueryStatement(sql);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
//            close();
        }
    }

    @Override
    public DBValResultSet executeQuery(String sql) throws SQLException {
        try {
            if (!apiEntry.isConnected()) open();
            return new IotDBResultSet(apiEntry.executeQueryStatement(sql));
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
//             close();
        }
    }
}
