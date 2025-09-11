/*
package com.fuzzy.common.query;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TSFuzzyResultSet implements Closeable {

    ResultSet rs;
    private Runnable runnableEpilogue;

    public TSFuzzyResultSet(ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public void close() {
        if (runnableEpilogue != null) {
            runnableEpilogue.run();
        }
        // TODO
        */
/*try {
            if (runnableEpilogue != null) {
                runnableEpilogue.run();
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            throw new AssertionError(e);
        }*//*

    }

    public boolean hasNext() throws SQLException {
        return rs.hasNext();
    }

    public int getInt(int i) throws SQLException {
        return rs.getInt(i);
    }

    public String getString(int i) throws SQLException {
        return rs.getString(i);
    }

    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }

    public long getLong(int i) throws SQLException {
        return rs.getLong(i);
    }

    public void registerEpilogue(Runnable runnableEpilogue) {
        this.runnableEpilogue = runnableEpilogue;
    }

}
*/
