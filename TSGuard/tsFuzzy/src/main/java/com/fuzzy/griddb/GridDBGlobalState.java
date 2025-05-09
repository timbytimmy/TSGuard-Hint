package com.fuzzy.griddb;

import com.fuzzy.SQLGlobalState;

import java.sql.SQLException;


public class GridDBGlobalState extends SQLGlobalState<GridDBOptions, GridDBSchema> {

    @Override
    protected GridDBSchema readSchema() throws SQLException {
        return GridDBSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == GridDBOptions.GridDBOracleFactory.PQS);
    }

    public boolean usesTSAF() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == GridDBOptions.GridDBOracleFactory.TSAF);
    }

}
