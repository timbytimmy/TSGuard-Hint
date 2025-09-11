package com.fuzzy.TDengine;

import com.fuzzy.SQLGlobalState;
import com.fuzzy.TDengine.TDengineOptions.TDengineOracleFactory;

import java.sql.SQLException;


public class TDengineGlobalState extends SQLGlobalState<TDengineOptions, TDengineSchema> {

    @Override
    protected TDengineSchema readSchema() throws SQLException {
        return TDengineSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == TDengineOracleFactory.PQS);
    }

    public boolean usesTSAF() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == TDengineOracleFactory.TemplateQS
                || o == TDengineOracleFactory.TSAF);
    }

}
