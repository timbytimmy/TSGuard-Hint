package com.fuzzy.iotdb;

import com.fuzzy.SQLGlobalState;
import com.fuzzy.iotdb.IotDBOptions.IotDBOracleFactory;

import java.sql.SQLException;

public class IotDBGlobalState extends SQLGlobalState<IotDBOptions, IotDBSchema> {

    @Override
    protected IotDBSchema readSchema() throws SQLException {
        return IotDBSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == IotDBOracleFactory.PQS);
    }

    public boolean usesTSAF() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == IotDBOracleFactory.TSAF);
    }

    public boolean usesHINT() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == IotDBOracleFactory.HINT);
    }

}
